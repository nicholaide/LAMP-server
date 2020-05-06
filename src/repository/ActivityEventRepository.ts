import { Database } from "../app"
import { ActivityEvent } from "../model/ActivityEvent"
import { _migrate_activity_event } from "./migrate"

export class ActivityEventRepository {
  public static async _select(
    id?: string,
    origin?: string,
    from_date?: number,
    to_date?: number,
    limit?: number
  ): Promise<ActivityEvent[]> {
    _migrate_activity_event()
    // FIXME: support activityspec origin + researcher/study id
    const all_res = (
      await Database.use("activity_event").find({
        selector: {
          "#parent": id!,
          activity: origin!,
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
  public static async _insert(participant_id: string, objects: ActivityEvent[]): Promise<{}> {
    //_migrate_activity_event() // TODO: DISABLED
    const data = await Database.use("activity_event").bulk({
      docs: objects.map((x) => ({
        "#parent": participant_id,
        timestamp: Number.parse(x.timestamp) ?? 0,
        duration: Number.parse(x.duration) ?? 0,
        activity: String(x.activity),
        static_data: x.static_data ?? {},
        temporal_slices: x.temporal_slices ?? [],
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
