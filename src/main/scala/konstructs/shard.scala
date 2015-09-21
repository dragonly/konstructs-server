package konstructs

import scala.collection.mutable

import akka.actor.{ Actor, Stash, ActorRef, Props }
import konstructs.api.{ BinaryLoaded, Position }

case class ChunkData(data: Array[Byte]) {
  import ChunkData._
  import Db._

  val version = data(0)

  def unpackTo(blockBuffer: Array[Byte]) {
    val size = compress.inflate(data, blockBuffer, Header, data.size - Header)
    assert(size == ChunkSize * ChunkSize * ChunkSize)
  }

  def block(c: ChunkPosition, p: Position, blockBuffer: Array[Byte]): Byte = {
    unpackTo(blockBuffer)
    blockBuffer(index(c, p))
  }

}

object ChunkData {
  import Db._
  def apply(blocks: Array[Byte], buffer: Array[Byte]): ChunkData = {
    val compressed = compress.deflate(blocks, buffer, Header)
    compressed(0) = Db.Version
    apply(compressed)
  }

  def index(x: Int, y: Int, z: Int): Int =
    x + y * Db.ChunkSize + z * Db.ChunkSize * Db.ChunkSize

  def index(c: ChunkPosition, p: Position): Int = {
    val x = p.x - c.p * Db.ChunkSize
    val y = p.y - c.k * Db.ChunkSize
    val z = p.z - c.q * Db.ChunkSize
    index(x, y, z)
  }

}

class ShardActor(db: ActorRef, shard: ShardPosition, val binaryStorage: ActorRef, chunkGenerator: ActorRef)
    extends Actor with Stash with utils.Scheduled with BinaryStorage {
  import ShardActor._
  import GeneratorActor._
  import DbActor._
  import Db._
  import BlockMetaActor._

  val ns = "chunks"

  private val blockBuffer = new Array[Byte](ChunkSize * ChunkSize * ChunkSize)
  private val compressionBuffer = new Array[Byte](ChunkSize * ChunkSize * ChunkSize + Header)
  private val chunks = new Array[Option[ChunkData]](ShardSize * ShardSize * ShardSize)

  private var dirty: Set[ChunkPosition] = Set()

  private def chunkId(c: ChunkPosition): String =
    s"${c.p}/${c.q}/${c.k}"

  private def chunkFromId(id: String): ChunkPosition = {
    val pqk = id.split('/').map(_.toInt)
    ChunkPosition(pqk(0), pqk(1), pqk(2))
  }

  schedule(5000, StoreChunks)

  def loadChunk(chunk: ChunkPosition): Option[ChunkData] = {
    val i = index(chunk)
    val blocks = chunks(i)
    if(blocks != null) {
      if(!blocks.isDefined)
        stash()
      blocks
    } else {
      loadBinary(chunkId(chunk))
      chunks(i) = None
      stash()
      None
    }
  }

  def readChunk(pos: Position)(read: Byte => Unit) = {
    val chunk = ChunkPosition(pos)
    loadChunk(chunk).map { c =>
      val block = c.block(chunk, pos, blockBuffer)
      read(block)
    }
  }

  def updateChunk(sender: ActorRef, pos: Position)(update: Byte => Byte) {
    val chunk = ChunkPosition(pos)
    loadChunk(chunk).map { c =>
      dirty = dirty + chunk
      c.unpackTo(blockBuffer)
      val i = ChunkData.index(chunk, pos)
      val oldBlock = blockBuffer(i)
      val block = update(oldBlock)
      blockBuffer(i) = block
      val data = ChunkData(blockBuffer, compressionBuffer)
      chunks(index(chunk)) = Some(data)
      sender ! BlockList(chunk, data)
    }
  }

  def receive() = {
    case SendBlocks(chunk) =>
      val s = sender
      loadChunk(chunk).map { c =>
        s ! BlockList(chunk, c)
      }
    case PutBlock(p, w, initiator) =>
      val s = sender
      updateChunk(initiator, p) { old =>
        if(old == 0) {
          w.toByte
        } else {
          s ! UnableToPut(p, w, initiator)
          old
        }
      }
    case ViewBlock(p, initiator) =>
      val s = sender
      readChunk(p) { w =>
        s ! BlockViewed(p, w.toInt, initiator)
      }
    case RemoveBlock(p, initiator) =>
      val s = sender
      updateChunk(initiator, p) { w =>
        s ! BlockRemoved(p, w, initiator)
        0
      }
    case ReplaceBlock(p, w, initiator) =>
      val s = sender
      updateChunk(initiator, p) { oldW =>
        s ! BlockRemoved(p, oldW, initiator)
        w.toByte
      }
    case BinaryLoaded(id, dataOption) =>
      val chunk = chunkFromId(id)
      dataOption match {
        case Some(data) =>
          chunks(index(chunk)) = Some(ChunkData(data))
          unstashAll()
        case None =>
          chunkGenerator ! Generate(chunk)
      }
    case Generated(position, data) =>
      chunks(index(position)) = Some(ChunkData(data, compressionBuffer))
      dirty = dirty + position
      unstashAll()
    case StoreChunks =>
      dirty.map { chunk =>
        chunks(index(chunk)).map { c =>
          storeBinary(chunkId(chunk), c.data)
        }
      }
      dirty = Set()
  }

}

object ShardActor {
  case object StoreChunks

  def index(c: ChunkPosition): Int = {
    val lp = math.abs(c.p % Db.ShardSize)
    val lq = math.abs(c.q % Db.ShardSize)
    val lk = math.abs(c.k % Db.ShardSize)
    lp + lq * Db.ShardSize + lk * Db.ShardSize * Db.ShardSize
  }

  def props(db: ActorRef, shard: ShardPosition,
    binaryStorage: ActorRef, chunkGenerator: ActorRef) =
    Props(classOf[ShardActor], db, shard, binaryStorage, chunkGenerator)
}
