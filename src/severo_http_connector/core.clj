(ns severo-http-connector.core
  (:gen-class)
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
    [muuntaja.core :as m]
    [ring.adapter.jetty :as jetty]
    [severo-http-connector
     [consumer :refer [consumer!]]
     [producer :refer [producer!]]])
  (:import
    [java.util UUID]))

(defn create-generic-handler
  [send-topic listen-topic timeout-ms poll-duration]
  (let [canal-producer (chan)
        cache (atom {})]
    (consumer! send-topic cache :duration poll-duration)
    (producer! listen-topic canal-producer)
    (fn [{:keys [parameters] :as req}]
      (log/debug "REQUEST: " req)
      (let [uuid (str (UUID/randomUUID))
            payload (assoc parameters :http-response-id uuid)
            canal-resposta (chan)]
        (swap! cache assoc uuid canal-resposta)
        (log/trace "cache: " @cache)
        (>!! canal-producer (json/generate-string payload))
        (let [[value channel] (alts!! [canal-resposta (timeout timeout-ms)])]
          (if value 
            (let [{:keys [http-status]} value]
              (log/info "RECEBEMOS O PAYLOAD " value)
              {:status http-status
               :headers {"Content-Type" "application/json"}
               :body (json/generate-string (apply dissoc value [:http-status :http-response-id]))})
            {:status 500
             :headers {"Content-Type" "application/json"}
             :body (json/generate-string {:message "Timeout."})}))))))

(defn prepare-reitit-handlers
  []
  (into [""
         {:swagger {:tags ["api"]}}]
        (mapv (fn [[route method-map]]
                [route (into {}
                             (mapv (fn [[method {:keys [send-topic listen-topic timeout poll-duration] :as conf}]]
                                     [method (-> conf
                                                 (dissoc :send-topic :listen-topic :timeout :poll-duration)
                                                 (assoc :parameters (if (#{:get :head} method)
                                                                      {:query map?}
                                                                      {:query map?
                                                                       :body map?}))
                                                 (assoc :handler (create-generic-handler send-topic listen-topic timeout poll-duration)))])
                                   method-map))])
              (:routes env))))

(defn start-web-service 
  []
  (let [app (http/ring-handler
              (http/router
                [["/swagger.json"
                  {:get {:no-doc true
                         :swagger {:info {:title (-> env :swagger :title)
                                          :description (-> env :swagger :description)}}
                         :handler (swagger/create-swagger-handler)}}]
                 (prepare-reitit-handlers)
                 ]

                {;:reitit.interceptor/transform dev/print-context-diffs ;; pretty context diffs
                 ;;:validate spec/validate ;; enable spec validation for route data
                 ;;:reitit.spec/wrap spell/closed ;; strict top-level validation
                 :exception pretty/exception
                 :data {:coercion reitit.coercion.spec/coercion
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
                  {:path "/"
                   :config {:validatorUrl nil
                            :operationsSorter "alpha"}})
                (ring/create-default-handler))
              {:executor sieppari/executor})]
    (jetty/run-jetty app {:port (or (-> env :http :port) 3000)
                          :join? true
                          :min-threads (or (-> env :http :min-threads) 16)
                          :max-threads (or (-> env :http :max-threads) 100)
                          :max-idle-time (or (-> env :http :max-idle-time) (* 60 1000 30))
                          :request-header-size (or (-> env :http :request-header-size) 8192)
                          })))





(comment 
  {:send-topic "poc"
   :listen-topic "poc"
   :timeout 2000
   :poll-duration 100;milliseconds
   } 
  (:routes env)
  (flatten 
    (into []
          (mapv (fn [route method-map]
                  (mapv (fn [method {:keys [send-topic listen-topic timeout poll-duration] :as conf}]
                          (let [reitit-map (-> conf
                                               (dissoc :send-topic :listen-topc :timeout :poll-duration)
                                               (assoc :handler (create-generic-handler send-topic listen-topic timeout-ms poll-duration)))]
                            retit-map))
                        method-map))
                (:routes env))))
  )

(def canal-producer (chan))
(def cache (atom {}))

(defn -main [& args]
  ;(log/info "criando consumidor")
  ;(consumer! "poc" cache)
  ;(log/info "criando produtor")
  ;(producer! "poc" canal-producer)

  ;handler http
  ;(let [uuid (str (UUID/randomUUID))
        ;payload {:http-response-id uuid :payload "lol"}
        ;canal-resposta (chan)]
    ;(swap! cache assoc uuid canal-resposta)
    ;(log/trace "cache: " @cache)
    ;(>!! canal-producer (json/generate-string payload))
    ;(let [[value channel] (alts!! [canal-resposta (timeout 2000)])]
      ;(if value 
        ;(log/info "RECEBEMOS O PAYLOAD " value)
        ;(log/info "Falha."))))
  (start-web-service)
  ) 
