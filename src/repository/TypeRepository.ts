import { Database, SQL } from "../app"
import ScriptRunner from "../utils/ScriptRunner"
import sql from "mssql"
import { DynamicAttachment } from "../model/Type"
import {
  Identifier_unpack,
  Participant_pack_id,
  Researcher_pack_id,
  Activity_unpack_id,
  Study_pack_id,
  _migrator_dual_table,
} from "./migrate"
import { CredentialRepository } from "./CredentialRepository"

export class TypeRepository {
  public static async _parent(type_id: string): Promise<{}> {
    const result: any = {} // obj['#parent'] === [null, undefined] -> top-level object
    for (const parent_type of await TypeRepository._parent_type(type_id))
      result[parent_type] = await TypeRepository._parent_id(type_id, parent_type)
    return result
  }

  /**
   * Get the self type of a given ID.
   */
  public static async _self_type(type_id: string): Promise<string> {
    try {
      await Database.use("participant").head(type_id)
      return "Participant"
    } catch (e) {}
    try {
      await Database.use("researcher").head(type_id)
      return "Researcher"
    } catch (e) {}
    try {
      await Database.use("study").head(type_id)
      return "Study"
    } catch (e) {}
    try {
      await Activity_self_type(type_id)
      //await Database.use("activity").head(type_id)
      return "Activity"
    } catch (e) {}
    try {
      await Database.use("sensor").head(type_id)
      return "Sensor"
    } catch (e) {}
    return "__broken_id__"
  }

  public static async _owner(type_id: string): Promise<string | null> {
    try {
      return ((await Database.use("participant").get(type_id)) as any)["#parent"]
    } catch (e) {}
    try {
      await Database.use("researcher").head(type_id)
      return null
    } catch (e) {}
    try {
      return ((await Database.use("study").get(type_id)) as any)["#parent"]
    } catch (e) {}
    try {
      return (await Activity_parent_id(type_id, "Study")) ?? null
      //return ((await Database.use("activity").get(type_id)) as any)["#parent"]
    } catch (e) {}
    try {
      return ((await Database.use("sensor").get(type_id)) as any)["#parent"]
    } catch (e) {}
    return null
  }

  /**
   * Get all parent types of a given ID.
   */
  public static async _parent_type(type_id: string): Promise<string[]> {
    const parent_types: { [type: string]: string[] } = {
      Researcher: [],
      Study: ["Researcher"],
      Participant: ["Study", "Researcher"],
      Activity: ["Study", "Researcher"],
      Sensor: ["Study", "Researcher"],
    }
    return parent_types[await TypeRepository._self_type(type_id)]
  }

  /**
   * Get a single parent object ID for a given ID.
   */
  public static async _parent_id(type_id: string, type: string): Promise<string> {
    const self_type: { [type: string]: Function } = {
      Researcher: Researcher_parent_id,
      Study: Study_parent_id,
      Participant: Participant_parent_id,
      Activity: Activity_parent_id,
      //Sensor: Sensor_parent_id,
    }
    return await (self_type[await TypeRepository._self_type(type_id)] as any)(type_id, type)
  }

  /**
   *
   */
  public static async _set(mode: "a" | "b", type: string, id: string, key: string, value?: DynamicAttachment | any) {
    const [, _export_table] = await _migrator_dual_table()
    id = _export_table[id] ?? id
    type = _export_table[type] ?? type
    let result: sql.IResult<any>
    if (mode === "a" && !value /* null | undefined */) {
      /* DELETE */ result = await SQL!.request().query(`
	            DELETE FROM LAMP_Aux.dbo.OOLAttachment
	            WHERE 
	                ObjectID = '${id}'
	                AND [Key] = '${key}'
	                AND ObjectType = '${type}';
			`)
    } else if (mode === "a" && !!value /* JSON value */) {
      /* INSERT or UPDATE */ const req = SQL!.request()
      req.input("json_value", sql.NVarChar, JSON.stringify(value))
      result = await req.query(`
	            MERGE INTO LAMP_Aux.dbo.OOLAttachment
	                WITH (HOLDLOCK) AS Output
	            USING (SELECT
	                '${type}' AS ObjectType,
	                '${id}' AS ObjectID,
	                '${key}' AS [Key]
	            ) AS Input(ObjectType, ObjectID, [Key])
	            ON (
	                Output.[Key] = Input.[Key] 
	                AND Output.ObjectID = Input.ObjectID 
	                AND Output.ObjectType = Input.ObjectType 
	            )
	            WHEN MATCHED THEN 
	                UPDATE SET Value = @json_value
	            WHEN NOT MATCHED THEN 
	                INSERT (
	                    ObjectType, ObjectID, [Key], Value
	                )
	                VALUES (
	                    '${type}', '${id}', '${key}', @json_value
	                );
			`)
    } else if (mode === "b" && !value /* null | undefined */) {
      /* DELETE */ result = await SQL!.request().query(`
	            DELETE FROM LAMP_Aux.dbo.OOLAttachmentLinker 
	            WHERE 
	                AttachmentKey = '${key}'
	                AND ObjectID = '${id}'
	                AND ChildObjectType = '${type}';
			`)
    } else if (mode === "b" && !!value /* DynamicAttachment */) {
      /* INSERT or UPDATE */ const { triggers, language, contents, requirements } = value
      const script_type = JSON.stringify({ language, triggers })
      const packages = JSON.stringify(requirements) || ""

      const req = SQL!.request()
      req.input("script_contents", sql.NVarChar, contents)
      result = await req.query(`
	            MERGE INTO LAMP_Aux.dbo.OOLAttachmentLinker 
	                WITH (HOLDLOCK) AS Output
	            USING (SELECT
	                '${key}' AS AttachmentKey,
	                '${id}' AS ObjectID,
	                '${type}' AS ChildObjectType
	            ) AS Input(AttachmentKey, ObjectID, ChildObjectType)
	            ON (
	                Output.AttachmentKey = Input.AttachmentKey 
	                AND Output.ObjectID = Input.ObjectID 
	                AND Output.ChildObjectType = Input.ChildObjectType 
	            )
	            WHEN MATCHED THEN 
	                UPDATE SET 
	                	ScriptType = '${script_type}',
	                	ScriptContents = @script_contents, 
	                	ReqPackages = '${packages}'
	            WHEN NOT MATCHED THEN 
	                INSERT (
	                    AttachmentKey, ObjectID, ChildObjectType, 
	                    ScriptType, ScriptContents, ReqPackages
	                )
	                VALUES (
	                    '${key}', '${id}', '${type}',
	                    '${script_type}', @script_contents, '${packages}'
	                );
			`)
    }
    return result!.rowsAffected[0] !== 0
  }

  /**
   * TODO: if key is undefined just return every item instead as an array
   */
  public static async _get(mode: "a" | "b", id: string, key: string): Promise<DynamicAttachment[] | any | undefined> {
    const [, _export_table] = await _migrator_dual_table()
    const _legacy_id = id
    id = _export_table[id!] ?? id!
    const components = Identifier_unpack(id)
    const from_type: string = components.length === 0 ? "Participant" : components[0]
    let parents = await TypeRepository._parent(_legacy_id)
    if (Object.keys(parents).length === 0) parents = { " ": " " } // for the SQL 'IN' operator

    if (mode === "a") {
      const result = (
        await SQL!.request().query(`
	            SELECT TOP 1 * 
	            FROM LAMP_Aux.dbo.OOLAttachment
	            WHERE [Key] = '${key}'
	                AND ((
	                	ObjectID = '${id}'
	                	AND ObjectType = 'me'
	                ) OR (
	                	ObjectID IN (${Object.values(parents)
                      .map((x) => `'${x}'`)
                      .join(", ")})
	                	AND ObjectType IN ('${from_type}', '${id}')
	                ));
			`)
      ).recordset

      if (result.length === 0) throw new Error("404.object-not-found")
      return JSON.parse(result[0].Value)
    } else if (mode === "b") {
      const result = (
        await SQL!.request().query(`
	            SELECT TOP 1 * 
	            FROM LAMP_Aux.dbo.OOLAttachmentLinker
	            WHERE AttachmentKey = '${key}'
	            	AND ((
	                	ObjectID = '${id}'
	                	AND ChildObjectType = 'me'
	                ) OR (
	                	ObjectID IN (${Object.values(parents)
                      .map((x) => `'${x}'`)
                      .join(", ")})
	                	AND ChildObjectType IN ('${from_type}', '${id}')
	                ));
			`)
      ).recordset
      if (result.length === 0) throw new Error("404.object-not-found")

      // Convert all to DynamicAttachments.
      return result.map((x) => {
        const script_type = x.ScriptType.startsWith("{")
          ? JSON.parse(x.ScriptType)
          : { triggers: [], language: x.ScriptType }

        const obj = new DynamicAttachment()
        obj.key = x.AttachmentKey
        obj.from = x.ObjectID
        obj.to = x.ChildObjectType
        obj.triggers = script_type.triggers
        obj.language = script_type.language
        obj.contents = x.ScriptContents
        obj.requirements = JSON.parse(x.ReqPackages)
        return obj
      })[0]
    }
  }

  public static async _list(mode: "a" | "b", id: string): Promise<string[]> {
    const [, _export_table] = await _migrator_dual_table()
    const _legacy_id = id
    id = _export_table[id!] ?? id!
    // Determine the parent type(s) of `type_id` first.
    const components = Identifier_unpack(id)
    const from_type: string = components.length === 0 ? "Participant" : components[0]
    let parents = await TypeRepository._parent(_legacy_id)
    if (Object.keys(parents).length === 0) parents = { " ": " " } // for the SQL 'IN' operator

    if (mode === "a") {
      // Request all static attachments.
      return (
        await SQL!.request().query(`
	            SELECT [Key]
	            FROM LAMP_Aux.dbo.OOLAttachment
	            WHERE (
	                	ObjectID = '${id}'
	                	AND ObjectType = 'me'
	                ) OR (
	                	ObjectID IN (${Object.values(parents)
                      .map((x) => `'${x}'`)
                      .join(", ")})
	                	AND ObjectType IN ('${from_type}', '${id}')
	                );
			`)
      ).recordset.map((x) => x.Key)
    } else {
      // Request all dynamic attachments.
      return (
        await SQL!.request().query(`
	            SELECT AttachmentKey
	            FROM LAMP_Aux.dbo.OOLAttachmentLinker
	            WHERE (
	                	ObjectID = '${id}'
	                	AND ChildObjectType = 'me'
	                ) OR (
	                	ObjectID IN (${Object.values(parents)
                      .map((x) => `'${x}'`)
                      .join(", ")})
	                	AND ChildObjectType IN ('${from_type}', '${id}')
	                );
			`)
      ).recordset.map((x) => x.AttachmentKey)
    }
  }

  /**
   *
   */
  public static async _invoke(attachment: DynamicAttachment, context: any): Promise<any | undefined> {
    if ((attachment.contents || "").trim().length === 0) return undefined

    // Select script runner for the right language...
    let runner: ScriptRunner
    switch (attachment.language) {
      case "rscript":
        runner = new ScriptRunner.R()
        break
      case "python":
        runner = new ScriptRunner.Py()
        break
      case "javascript":
        runner = new ScriptRunner.JS()
        break
      case "bash":
        runner = new ScriptRunner.Bash()
        break
      default:
        throw new Error("400.invalid-script-runner")
    }

    // Execute script.
    return await runner.execute(attachment.contents!, attachment.requirements!.join(","), context)
  }

  /**
   * FIXME: THIS FUNCTION IS DEPRECATED/OUT OF DATE/DISABLED (!!!)
   */
  public static async _process_triggers(): Promise<void> {
    console.log("Processing accumulated attachment triggers...")

    // Request the set of all updates.
    const accumulated_set = (
      await SQL!.request().query(`
			SELECT 
				Type, ID, Subtype, 
				DATEDIFF_BIG(MS, '1970-01-01', LastUpdate) AS LastUpdate, 
				Users.StudyId AS _SID,
				Users.AdminID AS _AID
			FROM LAMP_Aux.dbo.UpdateCounter
			LEFT JOIN LAMP.dbo.Users
				ON Type = 'Participant' AND Users.UserID = ID
			ORDER BY LastUpdate DESC;
		`)
    ).recordset.map((x) => ({
      ...x,
      _id:
        x.Type === "Participant"
          ? Participant_pack_id({ study_id: x._SID /*FIXME:Decrypt(<string>x._SID)*/ })
          : Researcher_pack_id({ admin_id: x.ID }),
      _admin:
        x.Type === "Participant" ? Researcher_pack_id({ admin_id: x._AID }) : Researcher_pack_id({ admin_id: x.ID }),
    }))
    const ax_set1 = accumulated_set.map((x) => x._id)
    const ax_set2 = accumulated_set.map((x) => x._admin)

    // Request the set of event masks prepared.
    const registered_set = (
      await SQL!.request().query(`
			SELECT * FROM LAMP_Aux.dbo.OOLAttachmentLinker; 
		`)
    ).recordset // TODO: SELECT * FROM LAMP_Aux.dbo.OOLTriggerSet;

    // Diff the masks against all updates.
    let working_set = registered_set.filter(
      (x) =>
        /* Attachment from self -> self. */
        (x.ChildObjectType === "me" && ax_set1.indexOf(x.ObjectID) >= 0) ||
        /* Attachment from self -> children of type Participant */
        (x.ChildObjectType === "Participant" && ax_set2.indexOf(x.ObjectID) >= 0) ||
        /* Attachment from self -> specific child Participant matching an ID */
        accumulated_set.find((y) => y._id === x.ChildObjectType && y._admin === x.ObjectID) !== undefined
    )

    // Completely delete all updates; we're done collecting the working set.
    // TODO: Maybe don't delete before execution?
    const result = await SQL!.request().query(`
            DELETE FROM LAMP_Aux.dbo.UpdateCounter;
		`)
    console.log("Resolved " + JSON.stringify(result.recordset) + " events.")

    // Duplicate the working set into specific entries.
    working_set = working_set
      .map((x) => {
        const script_type = x.ScriptType.startsWith("{")
          ? JSON.parse(x.ScriptType)
          : { triggers: [], language: x.ScriptType }

        const obj = new DynamicAttachment()
        obj.key = x.AttachmentKey
        obj.from = x.ObjectID
        obj.to = x.ChildObjectType
        obj.triggers = script_type.triggers
        obj.language = script_type.language
        obj.contents = x.ScriptContents
        obj.requirements = JSON.parse(x.ReqPackages)
        return obj
      })
      .map((x) => {
        // Apply a subgroup transformation only if we're targetting all
        // child resources of a type (i.e. 'Participant').
        if (x.to === "Participant")
          return accumulated_set
            .filter((y) => y.Type === "Participant" && y._admin === x.from && y._id !== y._admin)
            .map((y) => ({ ...x, to: y._id }))
        return [{ ...x, to: x.from as string }]
      })
    ;([] as any[]).concat(...working_set).forEach(async (x) =>
      TypeRepository._invoke(x, {
        /* The security context originator for the script 
				   with a magic placeholder to indicate to the LAMP server
				   that the script's API requests are pre-authenticated. */
        token: await CredentialRepository._packCosignerData(x.from, x.to),

        /* What object was this automation run for on behalf of an agent? */
        object: {
          id: x.to,
          type: TypeRepository._self_type(x.to),
        },

        /* Currently meaningless but does signify what caused the IA to run. */
        event: ["ActivityEvent", "SensorEvent"],
      })
        .then((y) => {
          TypeRepository._set("a", x.to, x.from as string, x.key + "/output", y)
        })
        .catch((err) => {
          TypeRepository._set(
            "a",
            x.to,
            x.from as string,
            x.key + "/output",
            JSON.stringify({ output: null, logs: err })
          )
        })
    )
    /* // TODO: This is for a single item only;
		let attachments: DynamicAttachment[] = await Promise.all((await TypeRepository._list('b', <string>type_id))
												.map(async x => (await TypeRepository._get('b', <string>type_id, x))))
		attachments
			.filter(x => !!x.triggers && x.triggers.length > 0)
			.forEach(x => TypeRepository._invoke(x).then(y => {
				TypeRepository._set('a', x.to!, <string>x.from!, x.key! + '/output')
			}))
		*/
  }
}

export async function Researcher_parent_id(id: string, type: string): Promise<string | undefined> {
  switch (type) {
    default:
      return undefined // throw new Error('400.invalid-identifier')
  }
}
export async function Study_parent_id(id: string, type: string): Promise<string | undefined> {
  switch (type) {
    case "Researcher":
      const obj: any = await Database.use("study").get(id)
      return obj["#parent"]
    default:
      throw new Error("400.invalid-identifier")
  }
}
export async function Participant_parent_id(id: string, type: string): Promise<string | undefined> {
  let obj: any
  switch (type) {
    case "Study":
      obj = await Database.use("participant").get(id)
      return obj["#parent"]
    case "Researcher":
      obj = await Database.use("participant").get(id)
      obj = await Database.use("study").get(obj["#parent"])
      return obj["#parent"]
    default:
      throw new Error("400.invalid-identifier")
  }
}
export async function Activity_self_type(id: string): Promise<void> {
  const [, _export_table] = await _migrator_dual_table()
  const _export_or_die = (id: string): string => {
    if (_export_table[id]) return _export_table[id]
    else throw new Error("500.invalid-migratable-id")
  }
  if (Identifier_unpack(_export_or_die(id))[0] !== "Activity") throw new Error()
}
export async function Activity_parent_id(id: string, type: string): Promise<string | undefined> {
  const [_lookup_table, _export_table] = await _migrator_dual_table()
  const _export_or_die = (id: string): string => {
    if (_export_table[id]) return _export_table[id]
    else throw new Error("500.invalid-migratable-id")
  }
  const { ctest_id, survey_id, group_id } = Activity_unpack_id(_export_or_die(id))
  switch (type) {
    case "Study":
    case "Researcher":
      if (survey_id > 0 /* survey */) {
        const result = (
          await SQL!.request().query(`
						SELECT AdminID AS value
						FROM Survey
						WHERE SurveyID = '${survey_id}'
					;`)
        ).recordset // IsDeleted = 0 AND
        return result.length === 0
          ? undefined
          : _lookup_table[
              (type === "Researcher" ? Researcher_pack_id : Study_pack_id)({
                admin_id: result[0].value,
              })
            ]
      } else if (ctest_id > 0 /* ctest */) {
        const result = (
          await SQL!.request().query(`
						SELECT AdminID AS value
						FROM Admin_CTestSettings
						WHERE AdminCTestSettingID = '${ctest_id}'
					;`)
        ).recordset // Status = 1 AND
        return result.length === 0
          ? undefined
          : _lookup_table[
              (type === "Researcher" ? Researcher_pack_id : Study_pack_id)({
                admin_id: result[0].value,
              })
            ]
      } else if (group_id > 0 /* group */) {
        const result = (
          await SQL!.request().query(`
						SELECT AdminID AS value
						FROM Admin_BatchSchedule
						WHERE AdminBatchSchID = '${group_id}'
					;`)
        ).recordset // IsDeleted = 0 AND
        return result.length === 0
          ? undefined
          : _lookup_table[
              (type === "Researcher" ? Researcher_pack_id : Study_pack_id)({
                admin_id: result[0].value,
              })
            ]
      } else return undefined
    default:
      throw new Error("400.invalid-identifier")
  }
}

/*
// Set up a 5-minute interval callback to invoke triggers.
setInterval(() => {
  if (!!SQL) TypeRepository._process_triggers()
}, 5 * 60 * 1000)
*/
