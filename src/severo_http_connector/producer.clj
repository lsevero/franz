(ns severo-http-connector.producer
  (:gen-class)
  (:require
    [cheshire.core :as json]
    [clojure.core.async
     :as a
     :refer [>! <! >!! <!! go chan buffer close! thread go-loop
             alts! alts!! timeout]]
    [clojure.tools.logging :as log] 
    [config.core :refer [env]]
    )
  (:import
    [java.util Properties]
    [org.apache.kafka.clients.admin AdminClient NewTopic]
    [org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord RecordMetadata]
    [org.apache.kafka.common.errors TopicExistsException]))

(def properties-producer
  (delay (doto (Properties.)
           (.putAll {ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
                     ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
                     ProducerConfig/BOOTSTRAP_SERVERS_CONFIG ^String (:kafka env)}))))

(defn producer! [topic canal-producer]
  (letfn [(create-topic! [topic partitions replication ^Properties cloud-config]
            (let [ac (AdminClient/create cloud-config)]
              (try
                (.createTopics ac [(NewTopic. topic partitions replication)])
                ;; Ignore TopicExistsException, which would get thrown if the topic was previously created
                (catch TopicExistsException e nil)
                (finally
                  (.close ac)))))
          (print-ex [e] (log/error e "Failed to deliver message."))
          (print-metadata [^RecordMetadata x]
            (log/info (format "Produced record to topic %s partition [%d] @ offest %d\n"
                              (.topic x)
                              (.partition x)
                              (.offset x))))]
    (let [producer (KafkaProducer. ^Properties @properties-producer)
          callback (reify Callback
                     (onCompletion [this metadata exception]
                       (if exception
                         (print-ex exception)
                         (print-metadata metadata))))]
      (create-topic! topic 1 3 @properties-producer)
      (go-loop []
               (let [record (ProducerRecord. topic (<! canal-producer))]
                 (log/debug "canal-producer recebeu " record)
                 (doto producer 
                   (.send record callback)
                   (.flush)))
               (recur)))))
