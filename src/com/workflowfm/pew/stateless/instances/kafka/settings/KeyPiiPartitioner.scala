package com.workflowfm.pew.stateless.instances.kafka.settings

import com.workflowfm.pew.stateless.instances.kafka.KafkaTopic.{KeyPiiCall, KeyPiiId, KeyPiiIdCall}
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.BsonCodecWrapper
import com.workflowfm.pew.stateless.instances.kafka.settings.bson.codecs.KeyPiiIdCodec
import org.apache.kafka.clients.producer.internals.DefaultPartitioner
import org.apache.kafka.common.Cluster
import org.bson.codecs.Codec

// https://console.bluemix.net/docs/services/MessageHub/messagehub112.html#partitioning
class KeyPiiPartitioner
  extends DefaultPartitioner {

  val codec: Codec[KeyPiiId] = new KeyPiiIdCodec
  val codecWrapper = new BsonCodecWrapper[KeyPiiId]( codec )

  override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    if ( key.isInstanceOf[KeyPiiCall] ) {
      val (keyPii, _) = key.asInstanceOf[KeyPiiIdCall]
      super.partition( topic, keyPii, codecWrapper.serialize( topic, keyPii ), value, valueBytes, cluster )

    } else super.partition( topic, key, keyBytes, value, valueBytes, cluster )
  }
}
