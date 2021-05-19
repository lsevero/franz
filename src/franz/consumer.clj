(ns franz.consumer
  (:require
    [clojure.core.async
     :as a
     :refer [>! go-loop]]
    [clojure.tools.logging :as log] 
    [franz.config :refer [config]])
  (:import
    [java.util Properties]
    [java.time Duration]
    [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer ConsumerRecord]
    ))

(defn properties-consumer ^Properties
  [cfg]
  (doto (Properties.)
    (.putAll (merge (-> config :kafka :consumer) (or cfg {})))))

(defn consumer!
  [topic consumer-cfg cache parse-fn & {:keys [duration] :or {duration 100}}]
  (let [consumer (KafkaConsumer. ^Properties (properties-consumer consumer-cfg))]
    (.subscribe consumer [topic])
    (go-loop [records []]
             (log/trace (str "records:" records))
             (doseq [^ConsumerRecord record records]
               (try
                 (let [{lisp-case :http-response-id
                        snake-case :http_response-id
                        :as value-record} (parse-fn (.value record))
                       http-response-id (or lisp-case snake-case)
                       key-record (.key record)
                       canal-resposta (get @cache http-response-id)
                       ]
                   (>! canal-resposta value-record)
                   (log/debug (format "Consumed record with key %s and value %s\n" key-record value-record)))
                 (catch Exception e nil)))
             (recur (seq (.poll consumer (Duration/ofMillis duration)))))))

