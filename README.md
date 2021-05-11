# severo-http-connector

A easy HTTP<->Kafka connector with a complete config file and swagger generation.

![http kafka converter](https://raw.githubusercontent.com/lsevero/severo-http-connector/master/http-kafka.jpeg)

## Usage

Download the `severo-http-connector-<VERSION>-standalone.jar` from the releases tab and run the jar with the config files with the command:

```bash
java -Dlog4j.configurationFile=<path-to>/log4j2.xml -Dconfig=<path-to>/config.edn -jar severo-http-connector-<VERSION>-standalone.jar
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
        :port 3000
        }
 ;Swagger options
 :swagger {:enabled? true ;swagger can be turned off setting this config to false. Defaults to true when it is not available.
           :title "SeveroHTTPConnector"
           :description "Connecting http to kafka"
           :path "/"}
 ;Any option that is not given at the `routes` is replaced to one the these default values
 :defaults {:send-topic "poc"
            :listen-topic "poc"
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
                         :timeout 2000 ;in milliseconds
                         ;The partitions and replication options are used to create the topic if it does not exist. If exists it will do nothing.
                         :partitions 1
                         :replication 3
                         ;Poll interval on the consumer loop
                         :poll-duration 100 ;in milisseconds 
                         :summary "test" ;swagger summary of the route
                         ;Here we can list and validate all the info given to the route.
                         ;This option is optional, if not given will pass the input to kafka without any validation
                         :parameters {:body ;Here we are using malli specs.
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
                        :timeout 200
                        :poll-duration 100
                        :parameters {:query [:map [:number [:and int? [:> 6]]]]}
                        :summary "testing get"
                        }}
          "/other-test" {:post {:send-topic "poc3"
                                 :listen-topic "poc3"
                                 :timeout 2000
                                 :poll-duration 100;milliseconds
                                 :summary "more test"
                                 ;We can define kafka configs per route as well
                                 ;these maps will be merged against the kafka configs above, per-route configs prevail
                                 :consumer {"group.id" "other-test"
                                           }
                                 :producer {}
                                 }}
          }
 }

```

## Logging

This project is using Log4j2 to log, and accepts a log4j2 compatible config file.
Besides the logs inside SeveroHTTPConnector you can extract the logs inside the kafka and http library, although they are very verbose.
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
