(ns severo-http-connector.consumer
  (:require
    [cheshire.core :as json]
    [clojure.core.async
     :as a
     :refer [>! go-loop]]
    [clojure.tools.logging :as log] 
    [config.core :refer [env]]
    )
  (:import
    [java.util Properties]
    [java.time Duration]
    [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer ConsumerRecord]
    ))

(def properties-consumer
  (delay (doto (Properties.)
           (.putAll {ConsumerConfig/GROUP_ID_CONFIG, "clojure_example_group"
                     ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                     ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                     ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG ^String (:kafka env)}))))

(defn consumer!
  [topic cache & {:keys [duration] :or {duration 100}}]
  (let [consumer (KafkaConsumer. ^Properties @properties-consumer)]
    (.subscribe consumer [topic])
    (go-loop [records []]
             (log/trace (str "records:" records))
             (doseq [^ConsumerRecord record records]
               (try
                 (let [{:keys [http-response-id] :as value-record} (json/parse-string (.value record) true)
                       key-record (.key record)
                       canal-resposta (get @cache http-response-id)
                       ]
                   (>! canal-resposta value-record)
                   (log/info "Consumed record with key %s and value %s\n" key-record value-record)
                   (log/trace (str "cache dentro do consumer: " cache))
                   (log/trace (str "canal-resposta: " canal-resposta))
                   )
                 (catch Exception e nil)))
             (recur (seq (.poll consumer (Duration/ofMillis duration)))))))

