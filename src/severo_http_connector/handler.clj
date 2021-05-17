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
               :body (json/generate-string {:message (.getMessage e)})}))))))

(defmethod create-generic-handler [:json :fire-and-forget]
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer]}]
  (let [canal-producer (chan)]
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
  [{:keys [send-topic listen-topic timeout poll-duration partitions replication consumer producer]
    {:keys [consumer-spec producer-spec]} :serialization}]
  (let [canal-producer (chan)
        producer-schema (avro/parse-schema (if (string? producer-spec)
                                             (-> producer-spec io/file io/input-stream)
                                             producer-spec))
        ]
    (producer! send-topic partitions replication producer canal-producer)
    (fn [{:keys [parameters headers] :as req}]
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
               :body (json/generate-string {:message (.getMessage e)})}))))))


(comment 
  (binding [abracad.avro.util/*mangle-names* false]
    (let [schema (avro/parse-schema
                 {:name "example", :type "record",
                  :fields [{:name "left", :type "string"}
                           {:name "right", :type "long"}]
                  })]
    (->> #_["foo" 31337]
         {:left "hue" :right 123}
         (avro/binary-encoded schema)
         (avro/decode schema)))
  
  (binding [abracad.avro.util/*mangle-names* true]
   (let [schema (clojure.reflect/reflect (avro/parse-schema {:type :record 
                                   :name :example_input 
                                   :fields [{:name :headers 
                                               :type {:type :map
                                                      :values :string}
                                               }
                                            {:name :uri 
                                             :type :string 
                                             }
                                            {:name :http-response-id 
                                             :type :string 
                                             }
                                            {:name :body 
                                               :type {:name :body_aux
                                                      :type :record
                                                      :fields [{:name :name
                                                                :type :string
                                                                }
                                                               {:name :age
                                                                :type :int
                                                                }
                                                               ]}
                                               }
                                            ]
                                   }))]
    (->> (json/parse-string
            "{\"body\":{\"age\":0,\"name\":\"string\"},\"http-response-id\":\"af8ef908-2ed1-4fce-a79f-9f4c193e9509\",\"headers\":{\"origin\":\"http://localhost:3000\",\"host\":\"localhost:3000\",\"accept_language\":\"pt_BR,pt;q=0.8,en_US;q=0.5,en;q=0.3\",\"cookie\":\"_xsrf=2|c5d3cd89|f0845c8d62d3eacf494790dc8ddb174e|1618867381; CSRF_Token_IAJOP=XPi9iGbaEihppyxe3PPDpnUSZNSJAkFn\",\"accept_encoding\":\"gzip, deflate\",\"referer\":\"http://localhost:3000/index.html\",\"connection\":\"keep_alive\",\"accept\":\"application/json\",\"content_length\":\"34\",\"content_type\":\"application/json\",\"user_agent\":\"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0\"},\"uri\":\"/avro\"}"
            true
           )
         #_{:body {:age 0, :name "string"},
          :http-response-id "af8ef908-2ed1-4fce-a79f-9f4c193e9509",
          :headers {:referer "http://localhost:3000/index.html",
                    :content_length "34",
                    :user_agent "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0",
                    :content_type "application/json",
                    :host "localhost:3000",
                    :accept_encoding "gzip, deflate",
                    :cookie "_xsrf=2|c5d3cd89|f0845c8d62d3eacf494790dc8ddb174e|1618867381; CSRF_Token_IAJOP=XPi9iGbaEihppyxe3PPDpnUSZNSJAkFn",
                    :origin "http://localhost:3000",
                    :accept_language "pt_BR,pt;q=0.8,en_US;q=0.5,en;q=0.3",
                    :connection "keep_alive",
                    :accept "application/json"},
          :uri "/avro"}
         (avro/binary-encoded schema)
         (avro/decode schema))
    ))

  
  
  (let [schema (avro/parse-schema 
                 "{
                 \"namespace\": \"example.avro\",
                 \"type\": \"record\",
                 \"name\": \"User\",
                 \"fields\": [
                 {\"name\": \"name\", \"type\": \"string\"},
                 {\"name\": \"favorite_number\",  \"type\": [\"null\", \"int\"]},
                 {\"name\": \"favorite_color\", \"type\": [\"null\", \"string\"]}
                 ] 
                 }
                " 
                 )]
    (->> {:name "hue"
          :favorite-number 7
          :favorite-color nil
          }
         (avro/binary-encoded schema)
         (avro/decode schema)
      
      
      )
    
    ))
  (spit "/tmp/avro" "{\"type\":\"record\",\"name\":\"example_input\",\"fields\":[{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"uri\",\"type\":\"string\"},{\"name\":\"http_response_id\"
,\"type\":\"string\"},{\"name\":\"body\",\"type\":{\"type\":\"record\",\"name\":\"body_aux\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}}]}")

  (spit "/tmp/fsaiohfsaduhfu" (json/generate-string {:type :record
                                                          :name 'output
                                                          :fields [{:name :headers
                                                                    :type {:type :map
                                                                           :values :string}
                                                                    }
                                                                   {:name :uri
                                                                    :type :string
                                                                    }
                                                                   {:name :http-response-id
                                                                    :type :string
                                                                    }
                                                                   {:name :body
                                                                    :type {:name :body-aux
                                                                           :type :record
                                                                           :fields [{:name :name
                                                                                     :type :string
                                                                                     }
                                                                                    {:name :age
                                                                                     :type :int
                                                                                     }
                                                                                    ]}
                                                                    }
                                                                   ]
                                                          }))
  )

(defn prepare-reitit-handlers
  []
  (into [""
         {:swagger {:tags ["api"]}}]
        (mapv (fn [[route method-map]]
                [route (into {}
                             (mapv (fn [[method {:keys [send-topic listen-topic timeout poll-duration
                                                        partitions replication consumer producer
                                                        serialization parameters mode]
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
                                                      mode :request-response
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
              (:routes env))))
