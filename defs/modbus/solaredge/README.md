# SolarEdge Modbus definitions

These definitions reproduce the fixed register maps used by
[EnergyExporter](https://github.com/MarcusWichelmann/EnergyExporter/tree/ea6c83be7a881243b3c1c36eaab8c13c01110119/src/EnergyExporter/Devices)
for an inverter, its first meter, and its first StorEdge battery. They preserve
EnergyExporter's measurement names, units and numeric status values.

| Definition (`meter` setting) | EnergyExporter configuration | Base address (decimal / hex) |
| --- | --- | --- |
| `solaredge/inverter` | `Type: Inverter`, `Index: 0` | 40000 / `0x9C40` |
| `solaredge/meter` | `Type: Meter`, `Index: 0` | 40121 / `0x9CB9` |
| `solaredge/battery` | `Type: Battery`, `Index: 0` | 57600 / `0xE100` |

The inverter supports the integer SunSpec model layout used by models 101–103;
it does not assume a particular phase count or hardware model. The meter uses
the fixed first-meter layout for integer models 201–204. These files do not
perform SunSpec model discovery or cover floating-point models, relocated model
blocks, additional meters, or a second battery. Check the reported `type` values
(inverter: 101–103; meter: 201–204) during commissioning.

All addresses are **zero-based Modbus PDU addresses**, read with function 03
(holding registers). For example, inverter AC power is register **40083**, not
40084. The latter is its base-one documentation address. The existing generic
SunSpec definitions are separate from these SolarEdge definitions.

SunSpec scale factors are signed 16-bit exponents, read before their values on
each poll. Battery strings keep normal Modbus byte order. Battery multi-register
numbers use `word_order: LowFirst`: the least significant 16-bit word comes first,
while bytes within each word remain big-endian. `HighFirst` remains the default
for existing definitions. Unscaled `UInt64` values are published as exact JSON
integers; scaled values and expression calculations use floating-point arithmetic.
The new word-order setting applies to reads; these definitions expose sensors
and do not define inverter or battery controls.

## Configuration

Merge the `modbus` section from [examples/solaredge.yaml](../../../examples/solaredge.yaml) into
`config/e2m.yaml`, or use the example as a starting configuration. Replace the
example inverter host and MQTT settings. For the Docker image, mount the
configuration directory at `/config` and build an image from this branch so it
includes the parser changes as well as the definitions:

```sh
docker build -t energy2mqtt:solaredge .
docker run --name energy2mqtt -p 8240:8240 \
  -v /absolute/path/to/config:/config:rw energy2mqtt:solaredge
```

The example uses **one hub**, with all three devices on **slave ID 1**, read every
**10 seconds**. Stop EnergyExporter before starting energy2mqtt against the
inverter: SolarEdge commonly permits only one active Modbus TCP client.

EnergyExporter's `SerialNumberOverride` is not required for device identity here.
energy2mqtt uses the configured device `name` for MQTT topics and Home Assistant
identifiers, independently of the `serial_number` measurement. Keep these names
stable even when a meter reports an incorrect serial number. The reported serial
number is still published as diagnostic data.

This is a register-map migration, not an MQTT compatibility layer. For the example,
state topics are:

- `energy2mqtt/devs/ModbusTCP/solaredge_inverter`
- `energy2mqtt/devs/ModbusTCP/solaredge_meter_1`
- `energy2mqtt/devs/ModbusTCP/solaredge_battery_1`

Existing MQTT consumers will need their topics and payload handling updated.
Meter import/export counters and signed power are forwarded with the source
map's meaning; verify the meter's installation role and orientation before using
these readings in energy calculations. Unimplemented phase registers and SunSpec
sentinel values are currently forwarded by the generic reader without filtering,
as in the source exporter; choose the applicable sensors for your installation.

## Validation and references

Run the tests without a physical inverter or MQTT broker:

```sh
cargo test --locked --no-default-features --features modbus --lib
```

The tests load all three YAML files and exercise actual Modbus TCP request and
response decoding with a local server, including signed scale factors, power,
energy counters, battery floats, and counters above the exact integer range of
`f64`. Internal scale factors are omitted from both state payloads and Home
Assistant discovery. Hardware validation remains necessary before deployment.

Register-map references:

- SolarEdge [SunSpec implementation technical note](https://knowledge-center.solaredge.com/sites/kc/files/sunspec-implementation-technical-note.pdf), using the **base 0** address column.
- EnergyExporter [SolarEdgeInverter.cs](https://github.com/MarcusWichelmann/EnergyExporter/blob/ea6c83be7a881243b3c1c36eaab8c13c01110119/src/EnergyExporter/Devices/SolarEdgeInverter.cs), [SolarEdgeMeter.cs](https://github.com/MarcusWichelmann/EnergyExporter/blob/ea6c83be7a881243b3c1c36eaab8c13c01110119/src/EnergyExporter/Devices/SolarEdgeMeter.cs), and [SolarEdgeBattery.cs](https://github.com/MarcusWichelmann/EnergyExporter/blob/ea6c83be7a881243b3c1c36eaab8c13c01110119/src/EnergyExporter/Devices/SolarEdgeBattery.cs).
- SolarEdge's [Power Control Open Protocol](https://github.com/MarcusWichelmann/EnergyExporter/blob/ea6c83be7a881243b3c1c36eaab8c13c01110119/docs/Power%20Control%20Open%20Protocol%20for%20SolarEdge%20Inverters.pdf), included in EnergyExporter, for the proprietary battery map.

The derived definitions retain EnergyExporter's MIT notice in
[LICENSE.EnergyExporter](LICENSE.EnergyExporter).
