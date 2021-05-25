(ns franz.producer
  (:gen-class)
  (:require
    [clojure.core.async
     :as a
     :refer [<! go-loop]]
    [clojure.tools.logging :as log] 
    [franz.config :refer [config]]
    )
  (:import
    [java.util Properties]
    [org.apache.kafka.clients.admin AdminClient NewTopic]
    [org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord RecordMetadata]
    [org.apache.kafka.common.errors TopicExistsException]))

(defn properties-producer ^Properties
  [cfg]
  (doto (Properties.)
    (.putAll (merge (-> config :kafka :producer) (or cfg {})))))

(defn producer! [topic partitions replication producer-cfg canal-producer flush?]
  (letfn [(create-topic! [^String topic partitions replication ^Properties cloud-config]
            (when (-> config :defaults :create-topic? true?)
              (if (and (some? partitions) (some? replication))
                (let [ac (AdminClient/create cloud-config)]
                  (try
                    (log/info (str "Creating topic " topic))
                    (.createTopics ac [(NewTopic. ^String topic ^int (int partitions) ^short (short replication))])
                    ;; Ignore TopicExistsException, which would get thrown if the topic was previously created
                    (catch TopicExistsException e nil)
                    (finally
                      (log/info (str "Topic " topic " already exists, nothing to do."))
                      (.close ac))))
                (log/warn (str "No partitions and replications was informed, cannot create topic " topic)))))

          (print-ex [e] (log/error e "Failed to deliver message."))

          (print-metadata [^RecordMetadata x]
            (log/debug (format "Produced record to topic %s partition [%d] @ offest %d\n"
                              (.topic x)
                              (.partition x)
                              (.offset x))))]

    (let [producer (KafkaProducer. ^Properties (properties-producer producer-cfg))
          callback (reify Callback
                     (onCompletion [this metadata exception]
                       (if exception
                         (print-ex exception)
                         (print-metadata metadata))))]
      (create-topic! topic partitions replication (properties-producer producer-cfg))
      (go-loop []
               (let [record (ProducerRecord. topic (<! canal-producer))]
                 (log/trace "canal-producer received: " record)
                 (if flush?
                   (doto producer 
                     (.send record callback)
                     (.flush))
                   (.send producer record callback)))
               (recur)))))
