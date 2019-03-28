import { SQL, Encrypt, Decrypt } from '../app'
import { 
	d, Schema, Property, Description, Retype, Route, Throws, 
	Path, BadRequest, NotFound, AuthorizationFailed, Auth,
	Enum, Ownership, Identifier, Parent, Body, Double, Int64, Timestamp
} from '../utils/OpenAPI'
import { IResult } from 'mssql'

@Schema()
@Description(d`
	
`)
export class AccessCitation {

	@Property()
	@Description(d`
		
	`)
	public in: string

	@Property()
	@Description(d`
		
	`)
	public at: string

	@Property()
	@Description(d`
		
	`)
	public on: Timestamp

	@Property()
	@Description(d`
		
	`)
	public by: string

	constructor(_in: string, at: string, on: Timestamp, by: string) {
		this.in = _in
		this.at = at
		this.on = on
		this.by = by
	}
}

@Schema()
@Description(d`
	
`)
export class Metadata {

	@Property()
	@Description(d`
		
	`)
	public access: AccessCitation

	constructor(access: AccessCitation) {
		this.access = access
	}
}

@Schema()
@Description(d`
	
`)
export class Document<T> {

	@Property()
	@Description(d`
		
	`)
	public meta: Metadata

	@Property()
	@Retype(Array, Object)
	@Description(d`
		
	`)
	public data: T[]

	constructor(meta: Metadata, data: T[]) {
		this.meta = meta
		this.data = data
	}
}

// https://en.wikipedia.org/wiki/Cron#CRON_expression
/*
* * * * * *
| | | | | | 
| | | | | +-- Year              (range: 1900-3000)
| | | | +---- Day of the Week   (range: 1-7; L=last, #=ordinal(range: 1-4))
| | | +------ Month of the Year (range: 1-12)
| | +-------- Day of the Month  (range: 1-31; L=last, W=nearest-weekday, #=ordinal(range: 1-52))
| +---------- Hour              (range: 0-23)
+------------ Minute            (range: 0-59)
*/
type CronDefinition = string;

@Schema()
@Description(d`
	
`)
export class DurationInterval {

	@Property()
	@Description(d`
		
	`)
	public start?: Timestamp

	@Property()
	@Retype(Array, String)
	@Description(d`
		
	`)
	public interval?: CronDefinition[]

	@Property()
	@Description(d`
		
	`)
	public repeat_count?: Int64

	@Property()
	@Description(d`
		
	`)
	public end?: Timestamp
}

enum RepeatTypeLegacy {
	hourly = 'hourly', // 0 * * * * *
	every3h = 'every3h', // 0 */3 * * * *
	every6h = 'every6h', // 0 */6 * * * *
	every12h = 'every12h', // 0 */12 * * * *
	daily = 'daily', // 0 0 * * * *
	weekly = 'weekly', // 0 0 * * 0 *
	biweekly = 'biweekly', // 0 0 1,15 * * *
	monthly = 'monthly', // 0 0 1 * * *
	bimonthly = 'bimonthly', // 0 0 1 */2 * *
	custom = 'custom', // 1 2 3 4 5 6
	none = 'none' // 0 0 0 0 0 0
}
Enum(RepeatTypeLegacy, d`
	The repeat type of a schedule.
`)

@Schema()
@Description(d`
	
`)
export class DurationIntervalLegacy {

	@Property()
	@Description(d`
		
	`)
	public repeat_type?: RepeatTypeLegacy

	@Property()
	@Description(d`
		
	`)
	public date?: Timestamp

	@Property()
	@Retype(Array, Timestamp)
	@Description(d`
		
	`)
	public custom_times?: Timestamp[]
}


// TODO: below is to convert legacy scheduling into modern cron-like versions
/* FIXME:
$obj->schedule = isset($raw->schedule) ? array_merge(...array_map(function($x) {
	$duration = new DurationInterval(); $ri = $x->repeat_interval;
	if ($ri >= 0 && $ri <= 4) { // hourly
		$h = ($ri == 4 ? 12 : ($ri == 3 ? 6 : ($ri == 2 ? 3 : 1)));
		$duration->interval = new CalendarComponents();
		$duration->interval->hour = $h;
	} else if ($ri >= 5 && $ri <= 10) { // weekly+
		if ($ri == 6) {
			$duration = [
				new DurationInterval(strtotime($x->time) * 1000, new CalendarComponents()), 
				new DurationInterval(strtotime($x->time) * 1000, new CalendarComponents())
			];
			$duration[0]->interval->weekday = 2;
			$duration[1]->interval->weekday = 4;
		} else if ($ri == 7) {
			$duration = [
				new DurationInterval(strtotime($x->time) * 1000, new CalendarComponents()), 
				new DurationInterval(strtotime($x->time) * 1000, new CalendarComponents()), 
				new DurationInterval(strtotime($x->time) * 1000, new CalendarComponents())
			];
			$duration[0]->interval->weekday = 1;
			$duration[1]->interval->weekday = 3;
			$duration[2]->interval->weekday = 5;
		} else {
			$duration = [
				new DurationInterval(strtotime($x->time) * 1000, new CalendarComponents())
			];
			$duration[0]->interval->day = ($ri == 5 ? 1 : null);
			$duration[0]->interval->week_of_month = ($ri == 9 ? 2 : ($ri == 8 ? 1 : null));
			$duration[0]->interval->month = ($ri == 10 ? 1 : null);
		}
	} else if ($ri == 11 && count($x->custom_time) == 1) { // custom+
		$duration->start = strtotime($x->custom_time[0]) * 1000;
		$duration->repeat_count = 1;
	} else if ($ri == 11 && count($x->custom_time) > 2) { // custom*
		$int_comp = (new DateTime($x->custom_time[0]))
						->diff(new DateTime($x->custom_time[1]));
		$duration->start = strtotime($x->custom_time[0]) * 1000;
		$duration->interval = new CalendarComponents();
		$duration->interval->year = ($int_comp->y == 0 ? null : $int_comp->y);
		$duration->interval->month = ($int_comp->m == 0 ? null : $int_comp->m);
		$duration->interval->day = ($int_comp->d == 0 ? null : $int_comp->d);
		$duration->interval->hour = ($int_comp->h == 0 ? null : $int_comp->h);
		$duration->interval->minute = ($int_comp->i == 0 ? null : $int_comp->i);
		$duration->interval->second = ($int_comp->s == 0 ? null : $int_comp->s);
		$duration->repeat_count = count($x->custom_time) - 1;
	} else if ($ri == 12) { // none
		$duration->start = strtotime($x->time) * 1000;
		$duration->repeat_count = 1;
	}
	return is_array($duration) ? $duration : [$duration];
}, $raw->schedule)) : null;
*/

// Schedule:
//      - Admin_CTestSchedule, Admin_SurveySchedule
//          - AdminID, CTestID/SurveyID, Version*(C), ScheduleDate, SlotID, Time, RepeatID, IsDeleted
//      - Admin_CTestScheduleCustomTime, Admin_SurveyScheduleCustomTime, Admin_BatchScheduleCustomTime
//          - Time
//      - Admin_BatchSchedule
//          - AdminID, BatchName, ScheduleDate, SlotID, Time, RepeatID, IsDeleted
//      - Admin_BatchScheduleCTest, Admin_BatchScheduleSurvey
//          - CTestID/SurveyID, Version*(C), Order
//
// Settings:
//      - Admin_CTestSurveySettings
//          - AdminID, CTestID, SurveyID
//      - Admin_JewelsTrailsASettings, Admin_JewelsTrailsBSettings
//          - AdminID, ... (")
//      - SurveyQuestions
//          - SurveyID, QuestionText, AnswerType, IsDeleted
//      - SurveyQuestionsOptions
//          - QuestionID, OptionText

/*
-- Utility function that removes keys from FOR JSON output.
-- i.e. UNWRAP_JSON([{'val':1,{'val':2},{'val':'cell'}], 'val') => [1,2,'cell']
CREATE OR ALTER FUNCTION FUNCTION
	dbo.UNWRAP_JSON(@json nvarchar(max), @key nvarchar(400)) RETURNS nvarchar(max)
AS BEGIN
	RETURN REPLACE(REPLACE(@json, FORMATMESSAGE('{"%s":', @key),''), '}','')
END;
*/