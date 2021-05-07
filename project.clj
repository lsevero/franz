(defproject severo-http-connector "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [cheshire "5.8.1"]
                 [yogthos/config "1.1.4"]
                 [ring/ring-jetty-adapter "1.7.1"]
                 [metosin/reitit "0.4.2"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [org.clojure/core.async "1.3.618"]
                 [org.clojure/tools.logging "1.1.0"]
                 [org.apache.logging.log4j/log4j-api "2.13.3"]
                 [org.apache.logging.log4j/log4j-core "2.13.3"]
                 [org.apache.logging.log4j/log4j-slf4j-impl "2.13.3"]
                 ]
  :main severo-http-connector.core/-main
  :profiles {:dev {:global-vars {*warn-on-reflection* true}
                   :plugins [[cider/cider-nrepl "0.25.4"]]
                   :resource-paths ["resources"
                                    "example-config"
                                    ]
                   }
             :uberjar {:aot :all}
             } 
  :repl-options {:init-ns severo-http-connector.core})
