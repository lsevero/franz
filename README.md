# severo-http-connector

A easy HTTP<->Kafka connector with a easy config file and swagger generation.

## Usage

Download the `severo-http-connector-<VERSION>-standalone.jar` from the releases tab and run the jar with the config files with the command:

```bash
java -Dlog4j.configurationFile=example-config/log4j2.xml -Dconfig=example-config/config.edn -jar target/severo-http-connector-1.0.0-standalone.jar
```

## License

Copyright © 2021 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
