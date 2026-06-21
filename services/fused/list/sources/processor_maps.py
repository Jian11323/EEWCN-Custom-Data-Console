from __future__ import annotations

from services.fused.list.sources.parsers.cenc_cwa import CencCwaParsers
from services.fused.list.sources.parsers.china_regional import ChinaRegionalParsers
from services.fused.list.sources.parsers.internal_ws import InternalWsParsers
from services.fused.list.sources.parsers.international import InternationalParsers


class DataSourceProcessor(
    ChinaRegionalParsers,
    InternationalParsers,
    CencCwaParsers,
    InternalWsParsers,
):
    """FanStudio / 内网数据源解析器（按区域拆分实现）。"""


# FanStudio数据源解析器映射
FAN_STUDIO_PARSERS = {
    "cenc": DataSourceProcessor.parse_cenc_data,
    "cwa": DataSourceProcessor.parse_cwa_fanstudio_data,
    "ningxia": DataSourceProcessor.parse_ningxia_data,
    "guangxi": DataSourceProcessor.parse_guangxi_data,
    "yunnan": DataSourceProcessor.parse_yunnan_data,
    "shanxi": DataSourceProcessor.parse_shanxi_data,
    "beijing": DataSourceProcessor.parse_beijing_data,
    "hko": DataSourceProcessor.parse_hko_data,
    "usgs": DataSourceProcessor.parse_usgs_data,
    "emsc": DataSourceProcessor.parse_emsc_data,
    "bcsf": DataSourceProcessor.parse_bcsf_data,
    "gfz": DataSourceProcessor.parse_gfz_data,
    "usp": DataSourceProcessor.parse_usp_data,
    "kma": DataSourceProcessor.parse_kma_data,
    "fssn": DataSourceProcessor.parse_fssn_data,
}

# 内网 ws 解析器
INTERNAL_WS_PARSERS = {
    "bmkg": DataSourceProcessor.parse_bmkg_fanstudio_data,
    "geonet": DataSourceProcessor.parse_geonet_fanstudio_data,
}
