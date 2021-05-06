(defproject severo-http-connector "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [cheshire "5.8.1"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [org.clojure/core.async "1.3.618"]
                 ]
  :main severo-http-connector.core/-main
  :repl-options {:init-ns severo-http-connector.core})
