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

(def app
  (http/ring-handler
    (http/router
      [["/swagger.json"
        {:get {:no-doc true
               :swagger {:info {:title "my-api"
                                :description "with reitit-http"}}
               :handler (swagger/create-swagger-handler)}}]

       ["/math"
        {:swagger {:tags ["math"]}}

        ["/plus"
         {:get {:summary "plus with data-spec query parameters"
                :parameters {:query {:x int?, :y int?}}
                :responses {200 {:body {:total pos-int?}}}
                :handler (fn [{{{:keys [x y]} :query} :parameters}]
                           {:status 200
                            :body {:total (+ x y)}})}
          :post {:summary "plus with data-spec body parameters"
                 :parameters {:body {:x int?, :y int?}}
                 :responses {200 {:body {:total int?}}}
                 :handler (fn [{{{:keys [x y]} :body} :parameters}]
                            {:status 200
                             :body {:total (+ x y)}})}}]

        ]]

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
    {:executor sieppari/executor}))

(defn start-web-service 
  []
  (jetty/run-jetty #'app {:port (-> env :http :port)
                          :join? true
                          :min-threads (or (-> env :http :min-threads) 16)
                          :max-threads (or (-> env :http :max-threads) 100)
                          :max-idle-time (or (-> env :http :max-idle-time) (* 60 1000 30))
                          :request-header-size (or (-> env :http :request-header-size) 8192)
                          }))

(def canal-producer (chan))
(def cache (atom {}))

(defn -main [& args]
  (log/info "criando consumidor")
  (consumer! "poc" cache)
  (log/info "criando produtor")
  (producer! "poc" canal-producer)

  ;;handler http
  (let [uuid (str (UUID/randomUUID))
        payload {:http-response-id uuid :payload "lol"}
        canal-resposta (chan)]
    (swap! cache assoc uuid canal-resposta)
    (log/debug "cache: " @cache)
    (>!! canal-producer (json/generate-string payload))
    (let [[value channel] (alts!! [canal-resposta (timeout 2000)])]
      (if value 
        (log/info "RECEBEMOS O PAYLOAD " value)
        (log/info "Falha."))))


  ) 
