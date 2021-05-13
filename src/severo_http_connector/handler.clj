(ns severo-http-connector.handler
  (:require
    [cheshire.core :as json]
    [abracad.avro :as avro]
    [clojure.core.async
     :as a
     :refer [>!! chan alts!!]]
    [clojure.tools.logging :as log] 
    [clojure.java.io :as io]
    [config.core :refer [env]]
    [severo-http-connector
     [consumer :refer [consumer!]]
     [producer :refer [producer!]] ])
  (:import
    [java.util UUID])
  (:gen-class))

(defmulti create-generic-handler (fn [cfg] (-> cfg :serialization :type)))

(defmethod create-generic-handler :defaulti
  [cfg]
  (throw (ex-info "There is no implementation for this configuration." {:cfg cfg})))

(defmethod create-generic-handler :json
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer]}]
  (let [canal-producer (chan)
        cache (atom {})]
    (consumer! listen-topic consumer cache #(try (json/parse-string % true)
                                                 (catch Exception e
                                                   (do (log/error e (str "Error decoding payload " % ". listen-topic:" listen-topic))
                                                       (throw e)))) :duration poll-duration)
    (producer! send-topic partitions replication producer canal-producer)
    (fn [{:keys [parameters headers] :as req}]
      (try
        (log/trace "http-request: " req)
        (let [uuid (str (UUID/randomUUID))
              payload (-> parameters
                          (assoc :http-response-id uuid)
                          (assoc :headers headers)
                          (assoc :uri (:uri req)))
              canal-resposta (chan)]
          (swap! cache assoc uuid canal-resposta)
          (log/trace "cache: " @cache)
          (>!! canal-producer (json/generate-string payload))
          (let [[value channel] (alts!! [canal-resposta (a/timeout timeout)])
                _ (swap! cache dissoc uuid)]
            (if value 
              (let [{:keys [http-status] :or {http-status 200}} value]
                (log/trace "consumed payload: " value)
                {:status http-status
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string (apply dissoc value [:http-status :http-response-id]))})
              {:status 504
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:message "Timeout."})})))
        (catch Exception e
          (do (log/error e "Unexpected exception")
              {:status 500
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:message (.getMessage e)})}))))))

(defmethod create-generic-handler :avro
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer]
    {:keys [consumer-spec producer-spec]} :serialization}]
  (let [canal-producer (chan)
        cache (atom {})
        consumer-schema (avro/parse-schema (if (string? consumer-spec)
                                             (-> consumer-spec io/file io/input-stream)
                                             consumer-spec))

        producer-schema (avro/parse-schema (if (string? producer-spec)
                                             (-> producer-spec io/file io/input-stream)
                                             producer-spec))
        ]
    (consumer! listen-topic consumer cache #(try (avro/decode consumer-schema %)
                                                 (catch Exception e
                                                   (do (log/error e (str "Error decoding payload with the given schema. listen-topic: " listen-topic))
                                                       (throw e)))) :duration poll-duration)
    (producer! send-topic partitions replication producer canal-producer)
    (fn [{:keys [parameters headers] :as req}]
      (try
        (log/trace "http-request: " req)
        (let [uuid (str (UUID/randomUUID))
              payload (-> parameters
                          (assoc :http-response-id uuid)
                          (assoc :headers headers)
                          (assoc :uri (:uri req)))
              canal-resposta (chan)]
          (swap! cache assoc uuid canal-resposta)
          (log/trace "cache: " @cache)
          (>!! canal-producer (try
                                (avro/binary-encoded payload)
                                (catch Exception e
                                  (do (log/error e (str "Error encoding the payload with the given schema. send-topic: " send-topic))
                                      (throw e)))))
          (let [[value channel] (alts!! [canal-resposta (a/timeout timeout)])
                _ (swap! cache dissoc uuid)]
            (if value 
              (let [{:keys [http-status] :or {http-status 200}} value]
                (log/trace "consumed payload: " value)
                {:status http-status
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string (apply dissoc value [:http-status :http-response-id]))})
              {:status 504
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:message "Timeout."})})))
        (catch Exception e
          (do (log/error e "Unexpected exception")
              {:status 500
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:message (.getMessage e)})}))))))

(comment 
  (let [schema (avro/parse-schema
                 {:name "example", :type "record",
                  :fields [{:name "left", :type "string"}
                           {:name "right", :type "long"}]
                  })]
    (->> #_["foo" 31337]
         {:left "hue" :right 123 :vish "seila"}
         (avro/binary-encoded schema)
         (avro/decode schema))))

(defn prepare-reitit-handlers
  []
  (into [""
         {:swagger {:tags ["api"]}}]
        (mapv (fn [[route method-map]]
                [route (into {}
                             (mapv (fn [[method {:keys [send-topic listen-topic timeout poll-duration
                                                        partitions replication consumer producer
                                                        serialization parameters]
                                                 :or {serialization {:type :json}
                                                      send-topic (or (-> env :defaults :send-topic)
                                                                     (throw (ex-info "send-topic cannot be null" {})))
                                                      listen-topic (or (-> env :defaults :listen-topic)
                                                                       (throw (ex-info "listen-topic cannot be null" {})))
                                                      timeout (or (-> env :defaults :timeout)
                                                                  (throw (ex-info "timeout cannot be null" {})))
                                                      poll-duration (or (-> env :defaults :poll-duration)
                                                                        (throw (ex-info "poll-duration cannot be null" {})))
                                                      partitions (or (-> env :defaults :partitions)
                                                                     (throw (ex-info "partitions cannot be null" {})))
                                                      replication (or (-> env :defaults :replication)
                                                                      (throw (ex-info "replication" {})))
                                                      consumer {}
                                                      producer {}}
                                                 :as conf}]]
                                     (let [conf-aux (-> conf
                                                        (dissoc :send-topic :listen-topic :timeout :poll-duration :partitions :replication :consumer :producer)
                                                        (assoc :handler (create-generic-handler {:serialization serialization
                                                                                                 :send-topic send-topic
                                                                                                 :listen-topic listen-topic
                                                                                                 :timeout timeout
                                                                                                 :poll-duration poll-duration
                                                                                                 :partitions partitions
                                                                                                 :replication replication
                                                                                                 :consumer consumer
                                                                                                 :producer producer
                                                                                                 })))
                                           conf-final (if parameters
                                                        conf-aux
                                                        (assoc conf-aux :parameters (if (#{:get :head} method)
                                                                                      {:query map?
                                                                                       :path map?
                                                                                       }
                                                                                      {:query map?
                                                                                       :path map?
                                                                                       :body map?})))]
                                       [method conf-final]))
                                   method-map))])
              (:routes env))))
