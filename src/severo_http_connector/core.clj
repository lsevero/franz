(ns severo-http-connector.core
  (:gen-class)
  (:require
    [cheshire.core :as json]
    [clojure.java.io :as jio]
    [clojure.core.async
     :as a
     :refer [>! <! >!! <!! go chan buffer close! thread go-loop
             alts! alts!! timeout]]
    [clojure.tools.logging :as log] 
    [config.core :refer [env]]
    )
  (:import
    [java.util Properties UUID]
    [java.time Duration]
    [org.apache.kafka.clients.consumer ConsumerConfig KafkaConsumer ConsumerRecord]
    [org.apache.kafka.clients.admin AdminClient NewTopic]
    [org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord RecordMetadata]
    [org.apache.kafka.common.errors TopicExistsException]))



(def ^Properties properties-consumer
  (delay (doto (Properties.)
           (.putAll {ConsumerConfig/GROUP_ID_CONFIG, "clojure_example_group"
                     ConsumerConfig/KEY_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                     ConsumerConfig/VALUE_DESERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringDeserializer"
                     ConsumerConfig/BOOTSTRAP_SERVERS_CONFIG ^String (:kafka env)}))))

(def ^Properties properties-producer
  (delay (doto (Properties.)
           (.putAll {ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
                     ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
                     ProducerConfig/BOOTSTRAP_SERVERS_CONFIG ^String (:kafka env)}))))

(def canal-producer (chan))
(def cache (atom {}))

(defn producer! [topic canal-producer]
  (letfn [(create-topic! [topic partitions replication ^Properties cloud-config]
            (let [ac (AdminClient/create cloud-config)]
              (try
                (.createTopics ac [(NewTopic. topic partitions replication)])
                ;; Ignore TopicExistsException, which would get thrown if the topic was previously created
                (catch TopicExistsException e nil)
                (finally
                  (.close ac)))))]
    (let [print-ex #(log/error % "Failed to deliver message.")
          print-metadata (fn [^RecordMetadata x]
                           (log/info (format "Produced record to topic %s partition [%d] @ offest %d\n"
                                             (.topic x)
                                             (.partition x)
                                             (.offset x))))
          producer (KafkaProducer. @properties-producer)
          callback (reify Callback
                     (onCompletion [this metadata exception]
                       (if exception
                         (print-ex exception)
                         (print-metadata metadata)))) 
          ]
      (create-topic! topic 1 3 @properties-producer)
      (go-loop []
               (let [record (ProducerRecord. topic (<! canal-producer))]
                 (log/debug "canal-producer recebeu " record)
                 (doto producer 
                   (.send record callback)
                   (.flush)))
               (recur))

      )))

(defn consumer! [topic cache]
  (let [consumer (KafkaConsumer. @properties-consumer)]
    (.subscribe consumer [topic])
    (go-loop [records []]
             (log/debug (str "records:" records))
             (doseq [^ConsumerRecord record records]
               (try
                 (let [{:keys [http-response-id] :as value-record} (json/parse-string (.value record) true)
                       key-record (.key record)
                       canal-resposta (get @cache http-response-id)
                       ]
                   (>! canal-resposta value-record)
                   (log/info "Consumed record with key %s and value %s\n" key-record value-record )
                   (log/debug (str "cache dentro do consumer: " cache))
                   (log/debug (str "canal-resposta: " canal-resposta))
                   )
                 (catch Exception e nil)))
             (recur (seq (.poll consumer (Duration/ofSeconds 1)))))))



(defn -main [& args]
  (log/info "criando consumidor")
  (consumer! "poc" cache)
  (log/info "criando produtor")
  (producer! "poc" canal-producer)
  (Thread/sleep 1000)

  ;;handler http
  (let [uuid (str (UUID/randomUUID))
        payload {:http-response-id uuid :payload "lol" }
        canal-resposta (chan)]
    (swap! cache assoc uuid canal-resposta)
    (log/debug "cache: " @cache)
    (>!! canal-producer (json/generate-string payload))
    (Thread/sleep 500)
    (let [[value channel] (alts!! [canal-resposta (timeout 2000)])]
      (if value 
        (log/info "RECEBEMOS O PAYLOAD " value)
        (log/info "Falha."))))


  ) 
