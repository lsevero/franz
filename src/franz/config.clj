(ns franz.config
  (:require [mount.core :as mount]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [aero.core :refer [read-config]]
            [malli.core :as malli]
            [malli.error :as me]
            [franz.spec :refer [config-spec]]
            ))

(defn get-config!
  []
  (let [config-path (System/getProperty "config")]
    (if (nil? config-path)
      (do (log/error "No config file was given.")
          (System/exit 1))
      (try
        (let [config (read-config config-path)]
          (log/info "Reading the config file...")
          (if-not (malli/validate config-spec config)
            (do
              (log/error (apply str (repeat 120 "=")))
              (log/error "Config file not valid!!!!")
              (log/error (-> config-spec
                             (malli/explain config)
                             me/with-spell-checking
                             me/humanize))
              (log/error (apply str (repeat 120 "=")))
              (System/exit 1))
            config))
        (catch Exception e
          (do (log/error e "Error validating the config file")
              (System/exit 1)))))))

(mount/defstate config
  :start (get-config!)
  :stop nil)
