import { Database } from "../app"
import { SensorEvent } from "../model/SensorEvent"
import { _migrate_sensor_event } from "./migrate"

export class SensorEventRepository {
  public static async _select(
    id?: string,
    origin?: string,
    from_date?: number,
    to_date?: number,
    limit?: number
  ): Promise<SensorEvent[]> {
    _migrate_sensor_event()
    // FIXME: support sensor origin + researcher/study id
    const all_res = (
      await Database.use("sensor_event").find({
        selector: {
          "#parent": id!,
          sensor: origin!,
          timestamp:
            from_date === undefined && to_date === undefined
              ? (undefined as any)
              : {
                  $gte: from_date,
                  $lt: from_date === to_date ? to_date! + 1 : to_date,
                },
        },
        sort: [
          {
            timestamp: !!limit && limit < 0 ? "asc" : "desc",
          },
        ],
        limit: Math.abs(limit ?? 1000),
      })
    ).docs.map((x) => ({
      ...x,
      _id: undefined,
      _rev: undefined,
      "#parent": undefined,
    })) as any
    return all_res
  }
  public static async _insert(participant_id: string, objects: SensorEvent[]): Promise<{}> {
    //_migrate_sensor_event() // TODO: DISABLED
    const data = await Database.use("sensor_event").bulk({
      docs: (objects as any[]).map((x) => ({
        "#parent": participant_id,
        timestamp: Number.parse(x.timestamp),
        sensor: String(x.sensor),
        data: x.data ?? {},
      })),
    })
    const output = data.filter((x) => !!x.error)
    if (output.length > 0) console.error(output)
    return {}
  }
  // eslint-disable-next-line
  public static async _delete(pid: string, origin?: string, from?: number, to?: number): Promise<{}> {
    throw new Error("503.unimplemented")
  }
}
