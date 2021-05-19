# Franz

A easy HTTP<->Kafka gateway with a complete config file and swagger generation.

![http kafka converter](https://raw.githubusercontent.com/lsevero/franz/master/http-kafka.jpeg)

## Usage

Download the `franz-<VERSION>-standalone.jar` from the releases tab and run the jar with the config files with the command:

```bash
java -Dlog4j.configurationFile=<path-to>/log4j2.xml -Dconfig=<path-to>/config.edn -jar franz-<VERSION>-standalone.jar
```
Check the config files in the `example-config` folder.

The kafka producer will insert a `http-response-id` field to the root of the input payload to be sent to kafka on the `send-topic`.
The HTTP connection will keep waiting for a message on the `listen-topic` with the `http-response-id` field, when we receive the message it will be returned to http.
The consumer optionally accepts a `http-status` which is the status code to be passed on the http response, if no http-status is given will be returned a 200.

## `config.edn`

Explained config.edn:
```clojure
{;Default kafka configs
;The kafka section contain all the java properties to control both consumer and producer. The same as the property files.
;they can be overwritten per route on the 'routes' section
 :kafka {:consumer {"bootstrap.servers" "localhost:9092" 
                    "group.id" "test"
                    "auto.offset.reset" "latest"
                    "enable.auto.commit" "true"
                    "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                    "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
                    }
         :producer {"bootstrap.servers" "localhost:9092"
                    "key.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                    "value.serializer" "org.apache.kafka.common.serialization.StringSerializer"
                    }}
 ;Here we control some option of the http server
 :http {:min-threads 16
        :max-threads 100
        :max-idle-time 1800000 
        :request-header-size 8192
        ;We are using the juxt/aero config library to read the config files
        ;we can pass environment variables inside the config file, if no venv has passed will default to the second argument of the #or clause
        :port #or [#env PORT 3000]
        }
 ;Swagger options
 :swagger {:enabled? true
           :title "Franz Gateway"
           :description "Connecting http to kafka"
           :path "/"}
 ;Any option that is not given at the `routes` is replaced to one the these default values
 :defaults {:create-topic? true; enable topic creation, set it to false to disable it. defaults to false.
            :send-topic "default"
            :listen-topic "default"
            :poll-duration 200
            :timeout 5000
            :partitions 1
            :replication 3
            }
 ;The list of all routes
 ;Each route can have multiple http methods with individual configs
 ;all of the options here are optional and if not given will default to the values above.
 :routes {"/test" {:post {:send-topic "poc"
                          :listen-topic "poc"
                          ;time to return a timeout on http
                          :timeout 2000
                          :partitions 1
                          :replication 3
                          ;Poll interval on the consumer loop
                          :poll-duration 100;milliseconds
                          :summary "test"  ;swagger summary of the route
                          ;Here we can list and validate all the info given to the route.
                          ;This option is optional, if not given will pass the input to kafka without any validation
                          :parameters {:body
                                       ;Here we are using malli specs.
                                       ;These specs will be documented on swagger
                                       [:and
                                        [:map
                                         [:x [:and int? [:> 6]]]
                                         [:y number?]]
                                        [:fn (fn [{:keys [x y]}] (> x y))]]
                                       :query [:map
                                               [:id string?]]}
                          }
                   :get {:send-topic "poc2"
                         :listen-topic "poc2"
                         :timeout 2000
                         :poll-duration 100
                         :parameters {:query [:map [:number [:and int? [:> 6]]]]}
                         :summary "testing get"
                         }}
          "/fire-and-forget/" {:post {:send-topic "poc3"
                                      :listen-topic "poc3"
                                      :timeout 2000
                                      :poll-duration 100;milliseconds
                                      :serialization {:type :json}
                                      :summary "fire and forget route"
                                      ; Operation mode, can be :request-response or :fire-and-forget, defaults to :request-response if not available
                                      :mode :fire-and-forget
                                      }}
          ; Avro serialization
          "/avro" {:post {:send-topic "avro"
                          ;We can define kafka configs per route as well
                          ;these maps will be merged against the kafka configs above, per-route configs prevail
                          :consumer {"group.id" "avro"
                                     "auto.offset.reset" "latest"
                                     "key.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                                     "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                                     }
                          :producer {"key.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
                                     "value.serializer" "org.apache.kafka.common.serialization.ByteArraySerializer"
                                     }
                          :listen-topic "avro"
                          :timeout 2000
                          :poll-duration 100
                          :summary "Avro serialization"
                          :parameters {:body [:map [:name string?]
                                                   [:age int?]]}
                          :serialization {:type :avro ;can be :json or :avro, defaults to :json
                                          ;specs can be or in the map representation, or a path to the json spec.
                                          ;We are using the damballa/abracad library to handle avro serialization
                                          ;by default it translates all the fields in snake_case to lisp-case, set it to false to  turn this behaviour off.
                                          :mangle true
                                          :producer-spec {:type :record
                                                          :name input
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
                                                          }
                                          ;path relative to the jar folder
                                          :consumer-spec "./example-config/avro.json"
                                          }
                          }}
          }
 }

```

## Logging

This project is using Log4j2 to log, and accepts a log4j2 compatible config file.
Besides the logs inside Franz you can extract the logs inside the kafka and http library, although they are very verbose.
Check the `example-config/log4j2.xml` file for a example.
The logging config is optional, although is recommended to use.

## Building

You'll need [leiningen](https://leiningen.org/) installed, then:
```bash
lein uberjar
```

## License

Copyright © 2021 Lucas Severo

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
