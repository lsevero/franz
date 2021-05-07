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
    [severo-http-connector
     [consumer :refer [consumer!]]
     [producer :refer [producer!]]]
    )
  (:import
    [java.util UUID]))

(def canal-producer (chan))
(def cache (atom {}))

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
