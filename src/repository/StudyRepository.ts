import { Database } from "../app"
import { Study } from "../model/Study"
import { _migrator_push_study, _migrator_pop_study } from "./migrate"
import { customAlphabet } from "nanoid"
const uuid = customAlphabet("1234567890abcdefghjkmnpqrstvwxyz", 20) // crockford-32

export class StudyRepository {
  public static async _select(id?: string): Promise<Study[]> {
    return (
      await Database.use("study").find({
        selector: !!id ? { $or: [{ _id: id }, { "#parent": id }] } : {},
        limit: 2_147_483_647 /* 32-bit INT_MAX */,
      })
    ).docs.map((doc: any) => ({
      id: doc._id,
      ...doc,
      _id: undefined,
      _rev: undefined,
      "#parent": undefined,
    }))
  }
  public static async _insert(researcher_id: string, object: Study): Promise<string> {
    const _id = uuid()
    await Database.use("study").insert({
      _id: _id,
      "#parent": researcher_id,
      name: object.name ?? "",
    } as any)
    _migrator_push_study(_id)
    return _id
  }
  public static async _update(study_id: string, object: Study): Promise<{}> {
    const orig: any = await Database.use("study").get(study_id)
    await Database.use("study").bulk({ docs: [{ ...orig, name: object.name ?? orig.name }] })
    return {}
  }
  public static async _delete(study_id: string): Promise<{}> {
    const orig: any = await Database.use("study").get(study_id)
    await Database.use("study").bulk({ docs: [{ ...orig, _deleted: true }] })
    _migrator_pop_study(study_id)
    return {}
  }
}
