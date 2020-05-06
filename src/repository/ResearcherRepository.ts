import { Database } from "../app"
import { Researcher } from "../model/Researcher"
import { _migrator_push_study, _migrator_pop_study } from "./migrate"
import { customAlphabet } from "nanoid"
const uuid = customAlphabet("1234567890abcdefghjkmnpqrstvwxyz", 20) // crockford-32

export class ResearcherRepository {
  public static async _select(id?: string): Promise<Researcher[]> {
    return (
      await Database.use("researcher").find({
        selector: !!id ? { _id: id } : {},
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
  public static async _insert(object: Researcher): Promise<string> {
    const _id = uuid()
    await Database.use("researcher").insert({
      _id: _id,
      name: object.name ?? "",
    } as any)
    // TODO: to match legacy behavior we create a default study as well
    const _id2 = uuid()
    await Database.use("study").insert({
      _id: _id2,
      "#parent": _id,
      name: object.name ?? "",
    } as any)
    _migrator_push_study(_id2)
    return _id
  }
  public static async _update(researcher_id: string, object: Researcher): Promise<{}> {
    const orig: any = await Database.use("researcher").get(researcher_id)
    await Database.use("researcher").bulk({ docs: [{ ...orig, name: object.name ?? orig.name }] })
    return {}
  }
  public static async _delete(researcher_id: string): Promise<{}> {
    const orig: any = await Database.use("researcher").get(researcher_id)
    await Database.use("researcher").bulk({ docs: [{ ...orig, _deleted: true }] })
    // TODO: to match legacy behavior we delete all child studies as well
    const studies = (
      await Database.use("study").find({
        selector: { "#parent": researcher_id },
        limit: 2_147_483_647 /* 32-bit INT_MAX */,
      })
    ).docs
    await Database.use("study").bulk({ docs: studies.map((x) => ({ ...x, _deleted: true })) })
    for (const x of studies) _migrator_pop_study(x._id)
    return {}
  }
}
