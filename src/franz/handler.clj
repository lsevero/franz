(ns franz.handler
  (:require
    [cheshire.core :as json]
    [abracad.avro :as avro]
    [clojure.core.async
     :as a
     :refer [>!! chan alts!!]]
    [clojure.tools.logging :as log] 
    [clojure.java.io :as io]
    [franz
     [config :refer [config]]
     [consumer :refer [consumer!]]
     [producer :refer [producer!]]])
  (:import
    [java.util UUID])
  (:gen-class))

(defmulti create-generic-handler (fn [cfg] 
                                   [(-> cfg :serialization :type) (:mode cfg)]))

(defmethod create-generic-handler :default
  [cfg]
  (throw (ex-info "There is no implementation for this configuration." {:cfg cfg})))

(defmethod create-generic-handler [:json :request-response]
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer flush?]}]
  (let [canal-producer (chan)
        cache (atom {})]
    (consumer! listen-topic consumer cache #(try (json/parse-string % true)
                                                 (catch Exception e
                                                   (do (log/error e (str "Error decoding payload " % ". listen-topic:" listen-topic))
                                                       (throw e)))) :duration poll-duration)
    (producer! send-topic partitions replication producer canal-producer flush?)
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
              (let [http-status (or (:http-status value)
                                    (:http_status value)
                                    200)]
                (log/trace "consumed payload: " value)
                {:status http-status
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string (apply dissoc value [:http-status :http-response-id :http_status :http_response_id]))})
              {:status 504
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:message "Timeout."})})))
        (catch Exception e
          (do (log/error e "Unexpected exception")
              {:status 500
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:message (.getMessage e)})}))))))

(defmethod create-generic-handler [:avro :request-response]
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer flush?]
    {:keys [consumer-spec producer-spec mangle]} :serialization}]
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
    (producer! send-topic partitions replication producer canal-producer flush?)
    (fn [{:keys [parameters headers] :as req}]
      (binding [abracad.avro.util/*mangle-names* (or mangle true)]
        (try
          (log/trace "http-request: " req)
          (let [uuid (str (UUID/randomUUID))
                payload (-> parameters
                            (assoc (if (or mangle true)
                                     :http-response-id
                                     :http_response_id
                                     ) uuid)
                            (assoc :headers headers)
                            (assoc :uri (:uri req)))
                canal-resposta (chan)]
            (swap! cache assoc uuid canal-resposta)
            (log/trace "cache: " @cache)
            (>!! canal-producer (try
                                  (avro/binary-encoded producer-schema payload)
                                  (catch Exception e
                                    (do (log/error e (str "Error encoding the payload with the given schema. send-topic: " send-topic))
                                        (throw e)))))
            (let [[value channel] (alts!! [canal-resposta (a/timeout timeout)])
                  _ (swap! cache dissoc uuid)]
              (if value 
                (let [http-status (or (:http-status value)
                                      (:http_status value)
                                      200)]
                  (log/trace "consumed payload: " value)
                  {:status http-status
                   :headers {"Content-Type" "application/json"}
                   :body (json/generate-string (apply dissoc value [:http-status :http-response-id :http_status :http_response_id]))})
                {:status 504
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string {:message "Timeout."})})))
          (catch Exception e
            (do (log/error e "Unexpected exception")
                {:status 500
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string {:message (.getMessage e)})})))))))

(defmethod create-generic-handler [:json :fire-and-forget]
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer flush?]}]
  (let [canal-producer (chan)]
    (producer! send-topic partitions replication producer canal-producer flush?)
    (fn [{:keys [parameters headers] :as req}]
      (try
        (log/trace "http-request: " req)
        (let [uuid (str (UUID/randomUUID))
              payload (-> parameters
                          (assoc :http-response-id uuid)
                          (assoc :headers headers)
                          (assoc :uri (:uri req)))
              canal-resposta (chan)]
          (>!! canal-producer (json/generate-string payload))
          {:status 200
           :headers {"Content-Type" "application/json"}
           :body (json/generate-string {:message "Sent"})})
        (catch Exception e
          (do (log/error e "Unexpected exception")
              {:status 500
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string {:message (.getMessage e)})}))))))

(defmethod create-generic-handler [:avro :fire-and-forget]
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer flush?]
    {:keys [consumer-spec producer-spec mangle]} :serialization}]
  (let [canal-producer (chan)
        producer-schema (avro/parse-schema (if (string? producer-spec)
                                             (-> producer-spec io/file io/input-stream)
                                             producer-spec))
        ]
    (producer! send-topic partitions replication producer canal-producer flush?)
    (fn [{:keys [parameters headers] :as req}]
      (binding [abracad.avro.util/*mangle-names* (or mangle true)]
        (try
          (log/trace "http-request: " req)
          (let [uuid (str (UUID/randomUUID))
                payload (-> parameters
                            (assoc :http-response-id uuid)
                            (assoc :headers headers)
                            (assoc :uri (:uri req)))
                ]
            (>!! canal-producer (try
                                  (avro/binary-encoded producer-schema payload)
                                  (catch Exception e
                                    (do (log/error e (str "Error encoding the payload with the given schema. send-topic: " send-topic))
                                        (throw e)))))
            {:status 200
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string {:message "Sent"})})
          (catch Exception e
            (do (log/error e "Unexpected exception")
                {:status 500
                 :headers {"Content-Type" "application/json"}
                 :body (json/generate-string {:message (.getMessage e)})})))))))

(defn prepare-reitit-handlers
  []
  (into [""
         {:swagger {:tags ["api"]}}]
        (mapv (fn [[route method-map]]
                [route (into {}
                             (mapv (fn [[method {:keys [send-topic listen-topic timeout poll-duration
                                                        partitions replication consumer producer
                                                        serialization parameters mode flush?]
                                                 :or {serialization {:type :json}
                                                      send-topic (or (-> config :defaults :send-topic)
                                                                     (throw (ex-info "send-topic cannot be null" {})))
                                                      listen-topic (or (-> config :defaults :listen-topic)
                                                                       (throw (ex-info "listen-topic cannot be null" {})))
                                                      timeout (or (-> config :defaults :timeout)
                                                                  (throw (ex-info "timeout cannot be null" {})))
                                                      poll-duration (or (-> config :defaults :poll-duration)
                                                                        (throw (ex-info "poll-duration cannot be null" {})))
                                                      partitions (-> config :defaults :partitions)
                                                      replication (-> config :defaults :replication)
                                                      mode :request-response
                                                      flush? (or (-> config :defaults :flush?) true)
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
                                                                                                 :flush? flush?
                                                                                                 :mode mode
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
              (:routes config))))
