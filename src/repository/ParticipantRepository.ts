import { Database } from "../app"
import { Participant } from "../model/Participant"
import { _migrator_push_participant, _migrator_pop_participant, _migrator_lookup_table } from "./migrate"

export class ParticipantRepository {
  public static async _select(id?: string): Promise<Participant[]> {
    // TODO: for legacy all_by_researcher support:
    const _studies = !!id
      ? (
          await Database.use("study").find({
            selector: { "#parent": id },
            limit: 2_147_483_647 /* 32-bit INT_MAX */,
          })
        ).docs.map((x) => ({ "#parent": x._id }))
      : []
    return (
      await Database.use("participant").find({
        selector: !!id ? { $or: [{ _id: id }, { "#parent": id }, ..._studies] } : {},
        limit: 2_147_483_647 /* 32-bit INT_MAX */,
      })
    ).docs.map((doc: any) => ({
      id: doc._id,
    }))
  }
  // eslint-disable-next-line
  public static async _insert(study_id: string, object: Participant): Promise<any> {
    const _id = `U${Math.random().toFixed(10).slice(2, 12)}`
    const actual_study_id = (await _migrator_lookup_table())[study_id] // FIXME
    if (actual_study_id === undefined) throw new Error("404.study-does-not-exist")
    try {
      await Database.use("participant").insert({ _id: _id, "#parent": actual_study_id } as any)
    } catch (e) {
      console.error(e)
      throw new Error("500.participant-creation-failed")
    }
    _migrator_push_participant(study_id, _id)
    return { id: _id }
  }
  // eslint-disable-next-line
  public static async _update(participant_id: string, object: Participant): Promise<{}> {
    throw new Error("503.unimplemented")
  }
  public static async _delete(participant_id: string): Promise<{}> {
    try {
      const orig = await Database.use("participant").get(participant_id)
      const data = await Database.use("participant").bulk({
        docs: [{ ...orig, _deleted: true }],
      })
      if (data.filter((x) => !!x.error).length > 0) throw new Error()
    } catch (e) {
      console.error(e)
      throw new Error("500.deletion-failed")
    }
    _migrator_pop_participant(participant_id)
    return {}
  }
}
