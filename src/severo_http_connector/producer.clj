(ns severo-http-connector.producer
  (:gen-class)
  (:require
    [cheshire.core :as json]
    [clojure.core.async
     :as a
     :refer [<! go-loop]]
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
           (.putAll (-> env :kafka :producer)))))

(defn producer! [^String topic ^long partitions ^long replication canal-producer]
  (letfn [(create-topic! [^String topic ^long partitions ^long replication ^Properties cloud-config]
            (let [ac (AdminClient/create cloud-config)]
              (try
                (.createTopics ac [(NewTopic. topic partitions replication)])
                ;; Ignore TopicExistsException, which would get thrown if the topic was previously created
                (catch TopicExistsException e nil)
                (finally
                  (.close ac)))))
          (print-ex [e] (log/error e "Failed to deliver message."))
          (print-metadata [^RecordMetadata x]
            (log/debug (format "Produced record to topic %s partition [%d] @ offest %d\n"
                              (.topic x)
                              (.partition x)
                              (.offset x))))]
    (let [producer (KafkaProducer. ^Properties @properties-producer)
          callback (reify Callback
                     (onCompletion [this metadata exception]
                       (if exception
                         (print-ex exception)
                         (print-metadata metadata))))]
      (create-topic! topic partitions replication @properties-producer)
      (go-loop []
               (let [record (ProducerRecord. topic (<! canal-producer))]
                 (log/trace "canal-producer received: " record)
                 (doto producer 
                   (.send record callback)
                   (.flush)))
               (recur)))))
