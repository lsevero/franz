(ns severo-http-connector.core
  (:gen-class)
  (:require
   [cheshire.core :as json]
   [clojure.java.io :as jio]
   [clojure.core.async
    :as a
    :refer [>! <! >!! <!! go chan buffer close! thread
            alts! alts!! timeout]])
  (:import
   (java.util Properties)
   (java.time Duration)
   (org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer)
   (org.apache.kafka.clients.admin AdminClient NewTopic)
   (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
   (org.apache.kafka.common.errors TopicExistsException)))


(defn- build-properties-producer []
  (with-open [config (-> "kafka.config" jio/resource jio/reader)]
    (doto (Properties.)
      (.putAll {ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
                ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"})
      (.load config))))

(defn- create-topic! [topic partitions replication cloud-config]
  (let [ac (AdminClient/create cloud-config)]
    (try
      (.createTopics ac [(NewTopic. topic partitions replication)])
      ;; Ignore TopicExistsException, which would get thrown if the topic was previously created
      (catch TopicExistsException e nil)
      (finally
        (.close ac)))))

(defn- build-properties-consumer []
  (with-open [config (-> "kafka.config" jio/resource jio/reader)]
    (doto (Properties.)
      (.putAll {ConsumerConfig/GROUP_ID_CONFIG, "clojure_example_group"
                ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"})
      (.load config))))

(defn producer! [topic]
  (let [props (build-properties-producer)
        print-ex (comp println (partial str "Failed to deliver message: "))
        print-metadata #(printf "Produced record to topic %s partition [%d] @ offest %d\n"
                                (.topic %)
                                (.partition %)
                                (.offset %))
        create-msg #(let [k "alice"
                          v (json/generate-string {:count %})]
                      (printf "Producing record: %s\t%s\n" k v)
                      (ProducerRecord. topic k v))]
    (with-open [producer (KafkaProducer. props)]
      (create-topic! topic 1 3 props)
      (let [;; We can use callbacks to handle the result of a send, like this:
            callback (reify Callback
                       (onCompletion [this metadata exception]
                         (if exception
                           (print-ex exception)
                           (print-metadata metadata))))]
        (doseq [i (range 5)]
          (.send producer (create-msg i) callback))
        (.flush producer)
        ;; Or we could wait for the returned futures to resolve, like this:
        (let [futures (doall (map #(.send producer (create-msg %)) (range 5 10)))]
          (.flush producer)
          (while (not-every? future-done? futures)
            (Thread/sleep 50))
          (doseq [fut futures]
            (try
              (let [metadata (deref fut)]
                (print-metadata metadata))
              (catch Exception e
                (print-ex e))))))
      (printf "10 messages were produced to topic %s!\n" topic))))

(defn consumer! [topic]
  (with-open [consumer (KafkaConsumer. (build-properties-consumer))]
    (.subscribe consumer [topic])
    (loop [tc 0
           records []]
      (let [new-tc (reduce
                    (fn [tc record]
                      (let [value (.value record)
                            cnt (get (json/parse-string value) "count")
                            new-tc (+ tc cnt)]
                        (printf "Consumed record with key %s and value %s, and updated total count to %d\n"
                                (.key record)
                                value
                                new-tc)
                        new-tc))
                        tc
                        records)]
        (println "Waiting for message in KafkaConsumer.poll")
        (recur new-tc
               (seq (.poll consumer (Duration/ofSeconds 1))))))))

(defn -main [topic & args]
  (println "criando consumidor")
  (.start (Thread. (partial consumer! topic)))
  (Thread/sleep 3000)
  (println "criando publisher")
  (.start (Thread. (partial producer! topic)))
  )
