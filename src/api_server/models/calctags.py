from typing import Any

from pydantic import BaseModel
from typing import Optional, Union
from custom_calc.models import ScriptInputTagData

from .durations import DataParamsSingleDuration


class _ScriptOutputTags(BaseModel):
    # Required
    tagname: str  
    script: str   
    display_tagname: str
    
    # Optional
    description: Optional[str] = None
    unit: Optional[str] = None
    systemidx: Optional[int] = None

    ai_rangelow: Optional[float] = -1000.0
    ai_rangehigh: Optional[float] = 1000.0

    ai_alarmhh: Optional[float] = 0
    ai_alarmhh_enable: Optional[bool] = False
    ai_alarmh: Optional[float] = 0
    ai_alarmh_enable: Optional[bool] = False
    ai_alarml: Optional[float] = 0
    ai_alarml_enable: Optional[bool] = False
    ai_alarmll: Optional[float] = 0
    ai_alarmll_enable: Optional[bool] = False

    di_alarm: Optional[int] = 1
    di_alarm_enable: Optional[bool] = False

    alarm_staytime: Optional[int] = 0
    alarmreactivatetime: Optional[int] = 0

    ignore_setting: Union[str, dict, None] = None
    ignore_enable: Optional[bool] = False
    
class _ScriptInfo(BaseModel):
    script_name: str
    initialization_code: str = None
    calculation_code: str
    input_tagnames: list[str]
    output_tags: list[_ScriptOutputTags]


class ScriptInfo(_ScriptInfo):
    script_id: int


class ScriptValidation(_ScriptInfo):
    data: list[dict[str, ScriptInputTagData]]


class ScriptData(BaseModel):
    script_id: int
    data: dict[str, ScriptInputTagData]


class ScriptDataPlot(BaseModel):
    script_id: int
    data_params: DataParamsSingleDuration


class RequestScriptCalc(BaseModel):
    script_id: int


class ResponseScriptCalc(BaseModel):
    script_id: int
    script_name: str
    input_data: list[_ScriptOutputTags]
    calc_result: dict[str, float]


class ResponseScriptCalcDebug(ResponseScriptCalc):
    calc_result_log: dict[str, Any]


class _ScriptExcutionTime(BaseModel):
    calc_result: dict[str, float]
    total_time: float
    calc_cnt: int
    mean_time: float


class ResponseScriptExecutionTime(BaseModel):
    script_id: int
    script_name: str
    message: str
    details: _ScriptExcutionTime = None
