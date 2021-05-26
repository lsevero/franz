(ns franz.core
  (:require
    [clojure.tools.logging :as log] 
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
    [muuntaja.core :as m]
    [ring.adapter.jetty :as jetty]
    [cheshire.core :as json]
    [mount.core :as mount]
    [franz
     [config :refer [config autoscale-threadpool]]
     [handler :refer [prepare-reitit-handlers]]
     ])
  (:import
    [java.util UUID])
  (:gen-class))

(defn swagger-http-server 
  []
  (let [app (http/ring-handler
              (http/router
                [["/swagger.json"
                  {:get {:no-doc true
                         :swagger {:info {:title (or (-> config :swagger :title) "Franz gateway")
                                          :description (or (-> config :swagger :description) "Connecting http to kafka")}}
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
                  {:path (or (-> config :swagger :path) "/")
                   :config {:validatorUrl nil
                            :operationsSorter "alpha"}})
                (ring/create-default-handler
                  {:not-found (constantly {:status 404
                                           :headers {"Content-Type" "application/json"}
                                           :body (json/generate-string {:message "Not found"})})
                   :method-not-allowed (constantly {:status 405
                                                    :headers {"Content-Type" "application/json"}
                                                    :body (json/generate-string {:message "Method not allowed"})})
                   :not-acceptable (constantly {:status 406
                                                :headers {"Content-Type" "application/json"}
                                                :body (json/generate-string {:message "Not acceptable"})})
                   }))
              {:executor sieppari/executor})]
    (jetty/run-jetty app {:port (or (-> config :http :port) 3000)
                          :join? true
                          :min-threads (or (-> config :http :min-threads) 8)
                          :max-threads (or (-> config :http :max-threads) 20)
                          :max-idle-time (or (-> config :http :max-idle-time) (* 60 1000 10))
                          :request-header-size (or (-> config :http :request-header-size) 8192)
                          })))

(defn http-server 
  []
  (let [app (http/ring-handler
              (http/router
                [(prepare-reitit-handlers)]

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
                (ring/create-default-handler
                  {:not-found (constantly {:status 404
                                           :headers {"Content-Type" "application/json"}
                                           :body (json/generate-string {:message "Not found"})})
                   :method-not-allowed (constantly {:status 405
                                                    :headers {"Content-Type" "application/json"}
                                                    :body (json/generate-string {:message "Method not allowed"})})
                   :not-acceptable (constantly {:status 406
                                                :headers {"Content-Type" "application/json"}
                                                :body (json/generate-string {:message "Not acceptable"})})
                   }))
              {:executor sieppari/executor})]
    (jetty/run-jetty app {:port (or (-> config :http :port) 3000)
                          :join? false
                          :min-threads (or (-> config :http :min-threads) 8)
                          :max-threads (or (-> config :http :max-threads) 20)
                          :max-idle-time (or (-> config :http :max-idle-time) (* 60 1000 10))
                          :request-header-size (or (-> config :http :request-header-size) 8192)
                          })))

(mount/defstate webserver
  :start (try
           (log/info "Config file is valid")
           (log/info "Initing web server")
           (if (false? (-> config :swagger :enabled?))
             (http-server)
             (swagger-http-server))
           (catch Exception e
             (do (log/error e "Error during the http server init.")
                 (System/exit 1))))
  :stop (do (log/info "Stopping webserver")
            (.stop ^org.eclipse.jetty.server.Server webserver)))

(defn -main [& args]
  (mount/start #'config
               #'autoscale-threadpool
               #'webserver)) 
