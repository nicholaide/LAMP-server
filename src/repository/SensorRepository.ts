import { Database } from "../app"
import { Sensor } from "../model/Sensor"

export class SensorRepository {
  public static async _select(id?: string): Promise<Sensor[]> {
    const data = await Database.use("sensor").list({ include_docs: true, start_key: id, end_key: id })
    return (data.rows as any).map((x: any) => ({
      id: x.doc._id,
      ...x.doc,
      _id: undefined,
      _rev: undefined,
    }))
  }
  // eslint-disable-next-line
  public static async _insert(id: string, object: Sensor): Promise<string> {
    throw new Error("503.unimplemented")
  }
  // eslint-disable-next-line
  public static async _update(sensor_spec_name: string, object: Sensor): Promise<string> {
    throw new Error("503.unimplemented")
  }
  // eslint-disable-next-line
  public static async _delete(sensor_spec_name: string): Promise<string> {
    throw new Error("503.unimplemented")
  }
}
