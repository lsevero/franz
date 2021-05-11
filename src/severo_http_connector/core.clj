(ns severo-http-connector.core
  (:require
    [cheshire.core :as json]
    [clojure.core.async
     :as a
     :refer [>! <! >!! <!! go chan buffer close! thread go-loop
             alts! alts!! timeout]]
    [clojure.tools.logging :as log] 
    [config.core :refer [env]]
    [reitit.ring :as ring]
    [reitit.http :as http]
    [reitit.coercion.spec]
    [reitit.swagger :as swagger]
    [reitit.swagger-ui :as swagger-ui]
    [reitit.http.coercion :as coercion]
    [reitit.dev.pretty :as pretty]
    [reitit.interceptor.sieppari :as sieppari]
    [reitit.http.interceptors.parameters :as parameters]
    [reitit.http.interceptors.muuntaja :as muuntaja]
    [reitit.http.interceptors.exception :as exception]
    [reitit.http.interceptors.multipart :as multipart]
    [reitit.coercion.malli]
    [reitit.ring.malli]
    [malli.util :as mu]
    [malli.core :as malli]
    [malli.error :as me]
    [muuntaja.core :as m]
    [ring.adapter.jetty :as jetty]
    [severo-http-connector
     [consumer :refer [consumer!]]
     [producer :refer [producer!]]])
  (:import
    [java.util UUID])
  (:gen-class))

(def get-route-spec
  [:map {:closed true}
   [:send-topic {:optional true} string?]
   [:listen-topic {:optional true} string?]
   [:poll-duration {:optional true} pos-int?]
   [:timeout {:optional true} pos-int?]
   [:partitions {:optional true} pos-int?]
   [:replication {:optional true} pos-int?]
   [:summary {:optional true} string?]
   [:consumer {:optional true} [:map-of :string :string]]
   [:producer {:optional true} [:map-of :string :string]]
   [:parameters {:optional true} [:map {:closed true}
                                  [:query any?]]]])

(def post-route-spec
  [:map {:closed true}
   [:send-topic {:optional true} string?]
   [:listen-topic {:optional true} string?]
   [:poll-duration {:optional true} pos-int?]
   [:timeout {:optional true} pos-int?]
   [:partitions {:optional true} pos-int?]
   [:replication {:optional true} pos-int?]
   [:summary {:optional true} string?]
   [:consumer {:optional true} [:map-of :string :string]]
   [:producer {:optional true} [:map-of :string :string]]
   [:parameters {:optional true} [:map {:closed true}
                                  [:query any?]
                                  [:body any?]]]])

(def config-spec
  [:map
   [:kafka [:map {:closed true}
            [:consumer [:map-of :string :string]]
            [:producer [:map-of :string :string]]
            ]]
   [:http [:and
           [:map
            [:min-threads {:optional true} pos-int?]
            [:max-threads {:optional true} pos-int?]
            [:max-idle-time {:optional true} pos-int?]
            [:request-header-size {:optional true} pos-int?]
            [:port {:optional true} [:and pos-int? [:< 65535]]]
            ]
           [:fn {:error/message "max-threads need to be greater than min-threads"} (fn [{:keys [min-threads max-threads]}] (> max-threads min-threads))]
           ]]
   [:swagger [:map {:closed true}
              [:title {:optional true} string?]
              [:description {:optional true} string?]
              [:path {:optional true} string?]
              ]]
   [:defaults [:map {:closed true}
               [:send-topic {:optional true} string?]
               [:listen-topic {:optional true} string?]
               [:poll-duration {:optional true} pos-int?]
               [:timeout {:optional true} pos-int?]
               [:partitions {:optional true} pos-int?]
               [:replication {:optional true} pos-int?]
               ]]
   [:routes 
    [:map-of :string [:and
                      [:map {:closed true}
                       [:get {:optional true} get-route-spec]
                       [:head {:optional true} get-route-spec]
                       [:post {:optional true} post-route-spec]
                       [:put {:optional true} post-route-spec]
                       [:delete {:optional true} post-route-spec]
                       [:connect {:optional true} post-route-spec]
                       [:options {:optional true} post-route-spec]
                       [:trace {:optional true} post-route-spec]
                       [:patch {:optional true} post-route-spec]]
                      [:fn {:error/message "method map cannot be empty"} (fn [m] (not (empty? m)))]]]]])

(defn create-generic-handler
  [send-topic listen-topic timeout-ms poll-duration partitions replication consumer-cfg producer-cfg]
  (let [canal-producer (chan)
        cache (atom {})]
    (consumer! listen-topic consumer-cfg cache :duration poll-duration)
    (producer! send-topic partitions replication producer-cfg canal-producer)
    (fn [{:keys [parameters] :as req}]
      (log/trace "http-request: " req)
      (let [uuid (str (UUID/randomUUID))
            payload (assoc parameters :http-response-id uuid)
            canal-resposta (chan)]
        (swap! cache assoc uuid canal-resposta)
        (log/trace "cache: " @cache)
        (>!! canal-producer (json/generate-string payload))
        (let [[value channel] (alts!! [canal-resposta (timeout timeout-ms)])
              _ (swap! cache dissoc uuid)]
          (if value 
            (let [{:keys [http-status] :or {http-status 200}} value]
              (log/trace "consumed payload: " value)
              {:status http-status
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string (apply dissoc value [:http-status :http-response-id]))})
            {:status 504
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string {:message "Timeout."})}))))))

(defn prepare-reitit-handlers
  []
  (into [""
         {:swagger {:tags ["api"]}}]
        (mapv (fn [[route method-map]]
                [route (into {}
                             (mapv (fn [[method {:keys [send-topic listen-topic timeout poll-duration
                                                        partitions replication consumer producer
                                                        parameters] :as conf}]]
                                     (let [conf-aux (-> conf
                                                        (dissoc :send-topic :listen-topic :timeout :poll-duration :partitions :replication :consumer :producer)
                                                        (assoc :handler (create-generic-handler (or send-topic
                                                                                                    (-> env :defaults :send-topic)
                                                                                                    (throw (ex-info "send-topic cannot be null" {})))
                                                                                                (or listen-topic
                                                                                                    (-> env :defaults :listen-topic)
                                                                                                    (throw (ex-info "listen-topic cannot be null" {})))
                                                                                                (or timeout
                                                                                                    (-> env :defaults :timeout)
                                                                                                    (throw (ex-info "timeout cannot be null" {})))
                                                                                                (or poll-duration
                                                                                                    (-> env :defaults :poll-duration)
                                                                                                    (throw (ex-info "poll-duration cannot be null" {})))
                                                                                                (or partitions
                                                                                                    (-> env :defaults :partitions)
                                                                                                    (throw (ex-info "partitions cannot be null" {})))
                                                                                                (or replication 
                                                                                                    (-> env :defaults :replication)
                                                                                                    (throw (ex-info "replication" {})))
                                                                                                (or consumer {})
                                                                                                (or producer {}))))
                                           conf-final (if parameters
                                                        conf-aux
                                                        (assoc conf-aux :parameters (if (#{:get :head} method)
                                                                                      {:query map?}
                                                                                      {:query map?
                                                                                       :body map?})))]

                                       [method conf-final]))
                                   method-map))])
              (:routes env))))

(defn start-web-service 
  []
  (let [app (http/ring-handler
              (http/router
                [["/swagger.json"
                  {:get {:no-doc true
                         :swagger {:info {:title (or (-> env :swagger :title) "SeveroHTTPConnector")
                                          :description (or (-> env :swagger :description) "Connecting http to kafka")}}
                         :handler (swagger/create-swagger-handler)}}]
                 (prepare-reitit-handlers)
                 ]

                {;:reitit.interceptor/transform dev/print-context-diffs ;; pretty context diffs
                 ;;:validate spec/validate ;; enable spec validation for route data
                 ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
                 :exception pretty/exception
                 :data {:coercion (reitit.coercion.malli/create
                                    {;; set of keys to include in error messages
                                     :error-keys #{#_:type :coercion :in :schema :value :errors :humanized #_:transformed}
                                     ;; schema identity function (default: close all map schemas)
                                     :compile mu/closed-schema
                                     ;; strip-extra-keys (effects only predefined transformers)
                                     :strip-extra-keys true
                                     ;; add/set default values
                                     :default-values true
                                     ;; malli options
                                     :options nil})
                        :muuntaja m/instance
                        :interceptors [;; swagger feature
                                       swagger/swagger-feature
                                       ;; query-params & form-params
                                       (parameters/parameters-interceptor)
                                       ;; content-negotiation
                                       (muuntaja/format-negotiate-interceptor)
                                       ;; encoding response body
                                       (muuntaja/format-response-interceptor)
                                       ;; exception handling
                                       (exception/exception-interceptor)
                                       ;; decoding request body
                                       (muuntaja/format-request-interceptor)
                                       ;; coercing response bodys
                                       (coercion/coerce-response-interceptor)
                                       ;; coercing request parameters
                                       (coercion/coerce-request-interceptor)
                                       ;; multipart
                                       (multipart/multipart-interceptor)]}})
              (ring/routes
                (swagger-ui/create-swagger-ui-handler
                  {:path (or (-> env :swagger :path) "/")
                   :config {:validatorUrl nil
                            :operationsSorter "alpha"}})
                (ring/create-default-handler))
              {:executor sieppari/executor})]
    (jetty/run-jetty app {:port (or (-> env :http :port) 3000)
                          :join? true
                          :min-threads (or (-> env :http :min-threads) 8)
                          :max-threads (or (-> env :http :max-threads) 20)
                          :max-idle-time (or (-> env :http :max-idle-time) (* 60 1000 10))
                          :request-header-size (or (-> env :http :request-header-size) 8192)
                          })))

(defn -main [& args]
  (try
    (log/info "Reading the config file...")
    (when-not (malli/validate config-spec env)
      (println (apply str (repeat 120 "=")))
      (println "Config file not valid!!!!")
      (println)
      (println (-> config-spec
                   (malli/explain env)
                   me/with-spell-checking
                   me/humanize))
      (println (apply str (repeat 120 "=")))
      (System/exit 1))
    (catch Exception e
      (do (log/error e "Error validating the config file")
          (System/exit 1))))
  (try
    (log/info "Config file is valid")
    (log/info "Initing web server")
    (start-web-service)
    (catch Exception e
      (do (log/error e "Error during the http server init.")
          (System/exit 1))))) 
