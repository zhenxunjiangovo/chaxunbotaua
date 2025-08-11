
import re
import time
import asyncio
import base64
import yaml
import json
import io

import uuid
import random
import shutil
import html
from datetime import datetime
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote, parse_qsl, urlencode, urlunparse
from typing import Optional

from telegram import Update, BotCommand, InputFile, InlineKeyboardButton, InlineKeyboardMarkup, Message, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler

from telegram.error import BadRequest, TimedOut, NetworkError

# ruamel.yaml 容错解析
try:
    from ruamel.yaml import YAML

    _HAS_RUAMEL = True
    yaml_ruamel = YAML(typ="safe")
except ImportError:
    _HAS_RUAMEL = False

# --- 全局配置常量 ---
CONCURRENT_LIMIT = 5000
TEXT_MESSAGE_URL_LIMIT = 500
FILE_OUTPUT_THRESHOLD = 5
QUERY_URL_LIMIT = 5000
NODE_DISPLAY_LIMIT = 30
CACHE_TTL_SECONDS = 30 * 60  # 内存缓存过期时间: 30分钟

# --- 全局配置 ---
CLIENT_USER_AGENTS = [
    "clash-verge-rev/2.3.2",
]

try:
    admin_id_str = '123456789'
    ADMIN_IDS = [int(admin_id.strip()) for admin_id in admin_id_str.split(',')]
    print(f"成功加载管理员ID: {ADMIN_ID}")
except ValueError:
    print("错误：ADMIN_ID 环境变量格式不正确，请确保是使用逗号分隔的数字。")
    ADMIN_IDS = []

# --- 文件与目录定义 ---
CACHE_FILE = "valid_subs.txt"
INVALID_CACHE_FILE = "invalid_subs.txt"
VALID_NODES_FILE = "valid_nodes.txt"
DOMAIN_MAP_FILE = "domain_map.txt"
TEMP_YAML_DIR = "temp_yaml_files"

# --- 正则表达式 ---
RE_FILENAME_UTF8 = re.compile(r"filename\*=UTF-8''(.+)")
RE_FILENAME_SIMPLE = re.compile(r'filename="?([^";]+)"?')
RE_URLS = re.compile(r"https?://[^\s'\"#]+")
RE_NODE_LINKS = re.compile(
    r"^(vmess|ssr|ss|shadowsocks|scocks5|http|https|http2|wireguard|ssh|trojan|snell|hysteria|hysteria2|tuic|anytls|vless)://",
    re.IGNORECASE
)
RE_DIRECT_NODE_LINKS = re.compile(
    r"^(vmess|ssr|ss|shadowsocks|scocks5|wireguard|ssh|trojan|snell|hysteria|hysteria2|tuic|anytls|vless)://",
    re.IGNORECASE
)
RE_USERINFO_KV = re.compile(r"(\w+)=(\d+)")

# --- 地区关键词 ---
COUNTRY_KEYWORDS = {
    '中国': ['CHINA', 'CHINANET', 'CHINA-NET', '中国', '🇨🇳', 'CN-', 'CN_', '[CN]', '中国节点', 'CN节点', '教育网',
            '长城', '联通', '电信', '移动', '广电', '北京', '上海', '天津', '重庆', '河北', '石家庄', '唐山', '秦皇岛',
            '邯郸', '邢台', '保定', '张家口', '承德', '沧州', '廊坊', '衡水', '山西', '太原', '大同', '阳泉', '长治',
            '晋城', '朔州', '晋中', '运城', '忻州', '临汾', '吕梁', '内蒙古', '呼和浩特', '包头', '乌海', '赤峰',
            '通辽', '鄂尔多斯', '呼伦贝尔', '巴彦淖尔', '乌兰察布', '兴安盟', '锡林郭勒盟', '阿拉善盟', '辽宁', '沈阳',
            '大连', '鞍山', '抚顺', '本溪', '丹东', '锦州', '营口', '阜新', '辽阳', '盘锦', '铁岭', '朝阳', '葫芦岛',
            '吉林', '长春', '吉林市', '四平', '辽源', '通化', '白山', '松原', '白城', '延边', '黑龙江', '哈尔滨',
            '齐齐哈尔', '鸡西', '鹤岗', '双鸭山', '大庆', '伊春', '佳木斯', '七台河', '牡丹江', '黑河', '绥化',
            '大兴安岭', '江苏', '南京', '无锡', '徐州', '常州', '苏州', '南通', '连云港', '淮安', '盐城', '扬州',
            '镇江', '泰州', '宿迁', '浙江', '杭州', '宁波', '温州', '嘉兴', '湖州', '绍兴', '金华', '衢州', '舟山',
            '台州', '丽水', '安徽', '合肥', '芜湖', '蚌埠', '淮南', '马鞍山', '淮北', '铜陵', '安庆', '黄山', '滁州',
            '阜阳', '宿州', '六安', '亳州', '池州', '宣城', '福建', '福州', '厦门', '莆田', '三明', '泉州', '漳州',
            '南平', '龙岩', '宁德', '江西', '南昌', '景德镇', '萍乡', '九江', '新余', '鹰潭', '赣州', '吉安', '宜春',
            '抚州', '上饶', '山东', '济南', '青岛', '淄博', '枣庄', '东营', '烟台', '潍坊', '济宁', '泰安', '威海',
            '日照', '临沂', '德州', '聊城', '滨州', '菏泽', '河南', '郑州', '开封', '洛阳', '平顶山', '安阳', '鹤壁',
            '新乡', '焦作', '濮阳', '许昌', '漯河', '三门峡', '南阳', '商丘', '信阳', '周口', '驻马店', '济源', '湖北',
            '武汉', '黄石', '十堰', '宜昌', '襄阳', '鄂州', '荆门', '孝感', '荆州', '黄冈', '咸宁', '随州', '恩施',
            '湖南', '长沙', '株洲', '湘潭', '衡阳', '邵阳', '岳阳', '常德', '张家界', '益阳', '郴州', '永州', '怀化',
            '娄底', '湘西', '广东', '广州', '深圳', '珠海', '汕头', '佛山', '韶关', '湛江', '肇庆', '江门', '茂名',
            '惠州', '梅州', '汕尾', '河源', '阳江', '清远', '东莞', '中山', '潮州', '揭阳', '云浮', '广西', '南宁',
            '柳州', '桂林', '梧州', '北海', '防城港', '钦州', '贵港', '玉林', '百色', '贺州', '河池', '来宾', '崇左',
            '海南', '海口', '三亚', '三沙', '儋州', '重庆', '四川', '成都', '自贡', '攀枝花', '泸州', '德阳', '绵阳',
            '广元', '遂宁', '内江', '乐山', '南充', '眉山', '宜宾', '广安', '达州', '雅安', '巴中', '资阳', '阿坝',
            '甘孜', '凉山', '贵州', '贵阳', '六盘水', '遵义', '安顺', '毕节', '铜仁', '黔西南', '黔东南', '黔南', '云南',
            '昆明', '曲靖', '玉溪', '保山', '昭通', '丽江', '普洱', '临沧', '楚雄', '红河', '文山', '西双版纳', '大理',
            '德宏', '怒江', '迪庆', '西藏', '拉萨', '昌都', '林芝', '山南', '日喀则', '那曲', '阿里', '陕西', '西安',
            '铜川', '宝鸡', '咸阳', '渭南', '延安', '汉中', '榆林', '安康', '商洛', '甘肃', '兰州', '嘉峪关', '金昌',
            '白银', '天水', '武威', '张掖', '平凉', '酒泉', '庆阳', '定西', '陇南', '临夏', '甘南', '青海', '西宁',
            '海东', '海北', '黄南', '海南', '果洛', '玉树', '海西', '宁夏', '银川', '石嘴山', '吴忠', '固原', '中卫',
            '新疆', '乌鲁木齐', '克拉玛依', '吐鲁番', '哈密', '昌吉', '博尔塔拉', '巴音郭楞', '阿克苏', '克孜勒苏', '喀什',
            '和田', '伊犁', '塔城', '阿勒泰', '直辖市', '自治州', '自治区', '内陆', '教育网', '科研网'],
		'香港': ['HK', 'HONG KONG', 'HONGKONG', '香港', '🇭🇰', 'HKG', '香 港', 'HK-', 'HK_', '香港・', 'HK |', '🇨🇳香港',
             'HONG-KONG', '[HK]', '(HK)', 'HK节点', '香港IEPL', 'HKIEPL', 'HK-IEPL', 'HONGKONGIEPL', 'hk节点',
             '香港节点'],
    '台湾': ['TW', 'TAIWAN', '台灣', '台湾', '臺灣', '🇹🇼', 'TW-', '台北', '臺北', 'TAIPEI', 'TPE', 'TW_', 'TW.', 'TW|',
             '[TW]', '台湾节点', 'TW节点', '中华电信', 'CHT', 'HINET'],
    '日本': ['JP', 'JAPAN', '日本', '東京', 'TOKYO', '大阪', 'OSAKA', '🇯🇵', 'JP-', 'JP_', 'JPN', 'JAPAN', '东京',
             '大坂', '[JP]', '日本节点', 'JP节点', '埼玉', 'SAITAMA', '名古屋', 'NAGOYA'],
    '新加坡': ['SG', 'SINGAPORE', '新加坡', '🇸🇬', 'SGP', 'SG-', 'SG_', 'Singapore', '新加坡节点', 'SG节点'],
    '韩国': ['KR', 'KOREA', '韩国', '首尔', 'SEOUL', '🇰🇷', 'KR-', 'KR_', '[KR]', '韩國', '韩国节点', 'KR节点'],
		'美国': ['US', 'USA', 'UNITED STATES', '美国', '美國', '美西', '美东', '🇺🇸', 'US-', 'US_', '[US]', '美国节点',
             'US节点', '纽约', '洛杉矶', '圣何塞', '硅谷', '华盛顿', '西雅图', '芝加哥', '达拉斯', '亚特兰大', '迈阿密',
             'NEW YORK', 'LOS ANGELES', 'SAN JOSE', 'SEATTLE', 'CHICAGO', 'DALLAS', 'ASHBURN'],
    '德国': ['DE', 'GERMANY', '德国', '🇩🇪', 'DE-', 'DE_', '[DE]', 'GER-', '德国节点', 'DE节点', '法兰克福',
             'FRANKFURT'],
    '英国': ['GB', 'UK', 'ENGLAND', 'LONDON', '英国', '英格兰', '倫敦', '🇬🇧', 'UK-', 'UK_', '[UK]', 'GB-', '伦敦',
             '英国节点', 'UK节点'],
    '俄罗斯': ['RU', 'RUSSIA', '俄罗斯', '🇷🇺', 'RU-', 'RU_', '[RU]', '俄羅斯', '俄罗斯节点', 'RU节点', '莫斯科',
               '圣彼得堡', '伯力', '新西伯利亚', 'MOSCOW'],
    '加拿大': ['CA', 'CANADA', '加拿大', '🇨🇦', 'CA-', 'CA_', '[CA]', '加拿大节点', 'CA节点', '多伦多', '温哥华',
               '蒙特利尔', 'TORONTO', 'VANCOUVER', 'MONTREAL', 'WATERLOO'],
    '澳大利亚': ['AU', 'AUSTRALIA', '澳大利亚', '澳洲', '🇦🇺', 'AU-', 'AU_', '[AU]', '澳大利亞',
                 '澳大利亚节点', 'AU节点', '悉尼', '墨尔本', 'SYDNEY', 'MELBOURNE'],
    '澳门': ['MACAU', 'MACAO', '澳门', '🇲🇴', 'MO-', 'MO_'],
    '马来西亚': ['MY', 'MALAYSIA', '马来西亚', '吉隆坡', 'KUALA LUMPUR', '🇲🇾', 'MY-', 'MY_', '[MY]', '馬來西亞',
                 '马来西亚节点', 'MY节点'],
    '泰国': ['TH', 'THAILAND', '泰国', '曼谷', 'BANGKOK', '🇹🇭', 'TH-', 'TH_', '[TH]', '泰國', '泰国节点', 'TH节点'],
    '越南': ['VN', 'VIETNAM', '越南', '河内', 'HANOI', '🇻🇳', 'VN-', 'VN_', '[VN]', '越南节点', 'VN节点'],
    '菲律宾': ['PH', 'PHILIPPINES', '菲律宾', '马尼拉', 'MANILA', '🇵🇭', 'PH-', 'PH_', '[PH]', '菲律賓', '菲律宾节点',
               'PH节点'],
    '印度尼西亚': ['ID', 'INDONESIA', '印度尼西亚', '雅加达', 'JAKARTA', '🇮🇩', 'ID-', 'ID_', '[ID]', '印尼',
                   '印度尼西亚节点', 'ID节点'],
    '印度': ['INDIA', '印度', '孟买', 'MUMBAI', '🇮🇳', 'IN-', 'IN_', '[IN]', '印度节点', 'IN节点', '新德里'],
    '柬埔寨': ['KH', 'CAMBODIA', '柬埔寨', '🇰🇭', 'KH-', 'KH_'],
    '土耳其': ['TR', 'TURKEY', '土耳其', '伊斯坦布尔', 'ISTANBUL', '🇹🇷', 'TR-', 'TR_', '[TR]', '土耳其节点', 'TR节点'],
    '阿联酋': ['AE', 'UAE', '阿联酋', '迪拜', 'DUBAI', '🇦🇪', 'AE-', 'AE_', '[AE]', '阿聯酋', '阿联酋节点', 'AE节点',
               'United Arab Emirates'],
    '沙特阿拉伯': ['SA', 'SAUDI ARABIA', '沙特', '沙特阿拉伯', '利雅得', 'RIYADH', '🇸🇦', 'SA-', 'SA_', '[SA]',
                   'SA节点'],
    '巴基斯坦': ['PK', 'PAKISTAN', '巴基斯坦', '🇵🇰', 'PK-', 'PK_', '[PK]', 'PK节点'],
    '以色列': ['IL', 'ISRAEL', '以色列', '耶路撒冷', 'JERUSALEM', '🇮🇱', 'IL-', 'IL_'],
    '卡塔尔': ['QA', 'QATAR', '卡塔尔', '多哈', 'DOHA', '🇶🇦', 'QA-', 'QA_'],
    '巴林': ['BAHRAIN', '巴林', '🇧🇭', 'BH-', 'BH_'],
    '孟加拉国': ['BD', 'BANGLADESH', '孟加拉', '🇧🇩', 'BD-', 'BD_'],
    '哈萨克斯坦': ['KZ', 'KAZAKHSTAN', '哈萨克斯坦', '🇰🇿', 'KZ-', 'KZ_'],
    '吉尔吉斯斯坦': ['KG', 'KYRGYZSTAN', '吉尔吉斯斯坦', '🇰🇬', 'KG-', 'KG_'],
    '乌兹别克斯坦': ['UZ', 'UZBEKISTAN', '乌兹别克斯坦', '🇺🇿', 'UZ-', 'UZ_'],
    '蒙古': ['MN', 'MONGOLIA', '蒙古', '🇲🇳', 'MN-', 'MN_'],
    '缅甸': ['MM', 'MYANMAR', '缅甸', '🇲🇲', 'MM-', 'MM_'],
    '尼泊尔': ['NP', 'NEPAL', '尼泊尔', '🇳🇵', 'NP-', 'NP_'],
    '老挝': ['LA', 'LAOS', '老挝', '🇱🇦', 'LA-', 'LA_'],
    '文莱': ['BN', 'BRUNEI', '文莱', '🇧🇳', 'BN-', 'BN_'],
    '约旦': ['JO', 'JORDAN', '约旦', '🇯🇴', 'JO-', 'JO_'],
    '黎巴嫩': ['LB', 'LEBANON', '黎巴嫩', '🇱🇧', 'LB-', 'LB_'],
    '阿曼': ['OM', 'OMAN', '阿曼', '🇴🇲', 'OM-', 'OM_'],
    '格鲁吉亚': ['GE', 'GEORGIA', '格鲁吉亚', '🇬🇪', 'GE-', 'GE_'],
    '亚美尼亚': ['AM', 'ARMENIA', '亚美尼亚', '🇦🇲', 'AM-', 'AM_'],
    '阿塞拜疆': ['AZ', 'AZERBAIJAN', '阿塞拜疆', '🇦🇿', 'AZ-', 'AZ_'],
    '叙利亚': ['SY', 'SYRIA', '叙利亚', '🇸🇾', 'SY-', 'SY_'],
    '伊拉克': ['IQ', 'IRAQ', '伊拉克', '🇮🇶', 'IQ-', 'IQ_'],
    '伊朗': ['IR', 'IRAN', '伊朗', '🇮🇷', 'IR-', 'IR_'],
    '阿富汗': ['AF', 'AFGHANISTAN', '阿富汗', '🇦🇫', 'AF-', 'AF_'],
    '墨西哥': ['MX', 'MEXICO', '墨西哥', '🇲🇽', 'MX-', 'MX_', '[MX]', '墨西哥节点', 'MX节点'],
    '巴西': ['BR', 'BRAZIL', '巴西', '🇧🇷', 'BR-', 'BR_', '[BR]', '巴西节点', 'BR节点', '圣保罗', 'SAO PAULO'],
    '阿根廷': ['AR', 'ARGENTINA', '阿根廷', '🇦🇷', 'AR-', 'AR_'],
    '智利': ['CL', 'CHILE', '智利', '🇨🇱', 'CL-', 'CL_'],
    '哥伦比亚': ['CO', 'COLOMBIA', '哥伦比亚', '🇨🇴', 'CO-', 'CO_'],
    '秘鲁': ['PE', 'PERU', '秘鲁', '🇵🇪', 'PE-', 'PE_'],
    '委内瑞拉': ['VE', 'VENEZUELA', '委内瑞拉', '🇻🇪', 'VE-', 'VE_'],
    '厄瓜多尔': ['EC', 'ECUADOR', '厄瓜多尔', '🇪🇨', 'EC-', 'EC_'],
    '乌拉圭': ['UY', 'URUGUAY', '乌拉圭', '🇺🇾', 'UY-', 'UY_'],
    '巴拉圭': ['PY', 'PARAGUAY', '巴拉圭', '🇵🇾', 'PY-', 'PY_'],
    '玻利维亚': ['BO', 'BOLIVIA', '玻利维亚', '🇧🇴', 'BO-', 'BO_'],
    '哥斯达黎加': ['CR', 'COSTA RICA', '哥斯达黎加', '🇨🇷', 'CR-', 'CR_'],
    '巴拿马': ['PA', 'PANAMA', '巴拿马', '🇵🇦', 'PA-', 'PA_'],
    '法国': ['FR', 'FRANCE', '法国', '🇫🇷', 'FR-', 'FR_', '[FR]', '法国节点', 'FR节点', '巴黎', 'PARIS'],
    '荷兰': ['NL', 'NETHERLANDS', '荷兰', '🇳🇱', 'NL-', 'NL_', '[NL]', '荷蘭', '荷兰节点', 'NL节点', '阿姆斯特丹',
             'AMSTERDAM'],
    '瑞士': ['CH', 'SWITZERLAND', '瑞士', '🇨🇭', 'CH-', 'CH_', '[CH]', '瑞士节点', 'CH节点', '苏黎世', 'ZURICH'],
    '意大利': ['ITALY', '意大利', '🇮🇹', 'IT-', 'IT_', '[IT]', '意大利节点', 'IT节点', '米兰', 'MILAN'],
    '西班牙': ['ES', 'SPAIN', '西班牙', '🇪🇸', 'ES-', 'ES_', '[ES]', '西班牙节点', 'ES节点', '马德里', 'MADRID'],
    '瑞典': ['SE', 'SWEDEN', '瑞典', '🇸🇪', 'SE-', 'SE_', '[SE]', '瑞典节点', 'SE节点'],
    '芬兰': ['FI', 'FINLAND', '芬兰', '🇫🇮', 'FI-', 'FI_', '[FI]', '芬蘭', '芬兰节点', 'FI节点'],
    '爱尔兰': ['IE', 'IRELAND', '爱尔兰', '🇮🇪', 'IE-', 'IE_', '[IE]', '愛爾蘭', '爱尔兰节点', 'IE节点', '都柏林',
               'DUBLIN'],
    '挪威': ['NO', 'NORWAY', '挪威', '🇳🇴', 'NO-', 'NO_', '[NO]', '挪威节点', 'NO节点', '奥斯陆', 'OSLO'],
    '丹麦': ['DK', 'DENMARK', '丹麦', '🇩🇰', 'DK-', 'DK_', '[DK]', '丹麥', '丹麦节点', 'DK节点'],
    '奥地利': ['AUSTRIA', '奥地利', '🇦🇹', 'AT-', 'AT_', '[AT]', '奧地利', '奥地利节点', 'AT节点'],
    '波兰': ['PL', 'POLAND', '波兰', '🇵🇱', 'PL-', 'PL_', '华沙', 'WARSAW'],
    '比利时': ['BE', 'BELGIUM', '比利时', '🇧🇪', 'BE-', 'BE_'],
    '捷克': ['CZ', 'CZECH', '捷克', '🇨🇿', 'CZ-', 'CZ_'],
    '匈牙利': ['HU', 'HUNGARY', '匈牙利', '🇭🇺', 'HU-', 'HU_'],
    '罗马尼亚': ['RO', 'ROMANIA', '罗马尼亚', '🇷🇴', 'RO-', 'RO_'],
    '乌克兰': ['UA', 'UKRAINE', '乌克兰', '🇺🇦', 'UA-', 'UA_'],
    '希腊': ['GR', 'GREECE', '希腊', '🇬🇷', 'GR-', 'GR_'],
    '葡萄牙': ['PT', 'PORTUGAL', '葡萄牙', '🇵🇹', 'PT-', 'PT_'],
    '保加利亚': ['BG', 'BULGARIA', '保加利亚', '🇧🇬', 'BG-', 'BG_'],
    '克罗地亚': ['HR', 'CROATIA', '克罗地亚', '🇭🇷', 'HR-', '🇭🇷_'],
    '爱沙尼亚': ['EE', 'ESTONIA', '爱沙尼亚', '🇪🇪', 'EE-', 'EE_'],
    '冰岛': ['ICELAND', '冰岛', '🇮🇸', 'IS-', 'IS_'],
    '拉脱维亚': ['LV', 'LATVIA', '拉脱维亚', '🇱🇻', 'LV-', 'LV_'],
    '立陶宛': ['LT', 'LITHUANIA', '立陶宛', '🇱🇹', 'LT-', 'LT_'],
    '卢森堡': ['LU', 'LUXEMBOURG', '卢森堡', '🇱🇺', 'LU-', 'LU_'],
    '塞尔维亚': ['RS', 'SERBIA', '塞尔维亚', '🇷🇸', 'RS-', 'RS_'],
    '斯洛伐克': ['SK', 'SLOVAKIA', '斯洛伐克', '🇸🇰', 'SK-', 'SK_'],
    '斯洛文尼亚': ['SI', 'SLOVENIA', '斯洛文尼亚', '🇸🇮', 'SI-', 'SI_'],
    '阿尔巴尼亚': ['AL', 'ALBANIA', '阿尔巴尼亚', '🇦🇱', 'AL-', 'AL_'],
    '摩尔多瓦': ['MD', 'MOLDOVA', '摩尔多瓦', '🇲🇩', 'MD-', 'MD_'],
    '波斯尼亚': ['BA', 'BOSNIA', '波黑', '🇧🇦', 'BA-', 'BA_'],
    '白俄罗斯': ['BY', 'BELARUS', '白俄罗斯', '🇧🇾', 'BY-', 'BY_'],
    '塞浦路斯': ['CY', 'CYPRUS', '塞浦路斯', '🇨🇾', 'CY-', 'CY_'],
    '马耳他': ['MT', 'MALTA', '马耳他', '🇲🇹', 'MT-', 'MT_'],
    '摩纳哥': ['MC', 'MONACO', '摩纳哥', '🇲🇨', 'MC-', 'MC_'],
    '列支敦士登': ['LI', 'LIECHTENSTEIN', '列支敦士登', '🇱🇮', 'LI-', 'LI_'],
    '黑山': ['ME', 'MONTENEGRO', '黑山', '🇲🇪', 'ME-', 'ME_'],
    '马其顿': ['MK', 'MACEDONIA', '马其顿', '🇲🇰', 'MK-', 'MK_'],
    '新西兰': ['NZ', 'NEW ZEALAND', '新西兰', '🇳🇿', 'NZ-', 'NZ_', '[NZ]', '新西蘭', '新西兰节点', 'NZ节点', '奥克兰',
               'AUCKLAND'],
    '斐济': ['FJ', 'FIJI', '斐济', '🇫🇯', 'FJ-', 'FJ_'],
    '南非': ['ZA', 'SOUTH AFRICA', '南非', '🇿🇦', 'ZA-', 'ZA_', '[ZA]', '南非节点', 'ZA节点', '约翰内斯堡',
             'JOHANNESBURG'],
    '埃及': ['EG', 'EGYPT', '埃及', '🇪🇬', 'EG-', 'EG_'],
    '尼日利亚': ['NG', 'NIGERIA', '尼日利亚', '🇳🇬', 'NG-', 'NG_'],
    '肯尼亚': ['KE', 'KENYA', '肯尼亚', '🇰🇪', 'KE-', 'KE_'],
    '加纳': ['GH', 'GHANA', '加纳', '🇬🇭', 'GH-', 'GH_'],
    '摩洛哥': ['MA', 'MOROCCO', '摩洛哥', '🇲🇦', 'MA-', 'MA_'],
    '阿尔及利亚': ['DZ', 'ALGERIA', '阿尔及利亚', '🇩🇿', 'DZ-', 'AL_'],
    '安哥拉': ['AO', 'ANGOLA', '安哥拉', '🇦🇴', 'AO-', 'AO_'],
    '突尼斯': ['TN', 'TUNISIA', '突尼斯', '🇹🇳', 'TN-', 'TN_'],
    '毛里求斯': ['MU', 'MAURITIUS', '毛里求斯', '🇲🇺', 'MU-', 'MU_'],
    '直连': ['直连', 'DIRECT'],
    '中转': ['中转', 'RELAY', 'TRANSFER', '隧道', 'TUNNEL', '公网中转', '海外', '国内', '入口', '出口'],
    '专线': ['专线', 'IPLC', 'IEPL', '专', '内网', 'SD-WAN', 'PRIVATE LINE'],
    'BGP': ['BGP'],
    'CDN': ['CDN'],
    '未知': ['未知', 'UNKNOWN'],
}
ORDERED_COUNTRIES = list(COUNTRY_KEYWORDS.keys())


# --- 辅助函数 ---
def safe_html(text: str) -> str:
    if not isinstance(text, str):
        text = str(text)
    return html.escape(text)


async def send_notification_to_admin(bot: Bot, update: Update, results: list):
    """[修改2] 格式化详细信息并推送到管理员的功能"""
    try:
        user = update.effective_user
        chat = update.effective_chat
        message = update.effective_message

        user_info_str = f"<b>用户:</b> {safe_html(user.full_name)}"
        if user.username:
            user_info_str += f" (@{safe_html(user.username)})"
        user_info_str += f" (ID: <code>{user.id}</code>)"

        if chat.type == "private":
            chat_info_str = "<b>来源:</b> <code>私聊</code>"
        else:
            chat_link = message.link
            chat_title_safe = safe_html(chat.title or '未知群组')
            chat_info_str = f"<b>来源群组:</b> <a href='{chat_link}'>{chat_title_safe}</a> (ID: <code>{chat.id}</code>)"

        header_info = f"<b>订阅查询通知</b>\n\n{user_info_str}\n{chat_info_str}\n"

        valid_results = [r for r in results if
                         r and r.get('status', {}).get('valid') and not r.get('status', {}).get('exhausted') and not
                         r.get('status', {}).get('expired')]

        is_doc_from_user = update.message.document is not None

        if is_doc_from_user and len(valid_results) > 5:
            summary_text = header_info + f"\n用户上传的文件产生了 {len(valid_results)} 个有效订阅，详情见附件。"
            await bot.send_message(chat_id=NOTIFICATION_ID, text=summary_text, parse_mode="HTML")

            def clean_html_for_file(html_text: str) -> str:
                # 1. 先将 <pre><code> 这种特殊结构转换成换行
                text_with_newlines = html_text.replace('<pre><code>', '\n').replace('</code></pre>', '')
                # 2. 使用通用的正则表达式，移除所有 <...> 形式的HTML标签
                plain_text = re.sub(r'<[^>]+>', '', text_with_newlines)
                # 3. 处理像 &amp; 这样的HTML实体编码，并去除首尾多余的空白
                return html.unescape(plain_text).strip()

            file_content = "\n\n".join(clean_html_for_file(r['summary_text']) for r in valid_results).encode('utf-8')
            await bot.send_document(
                chat_id=NOTIFICATION_ID,
                document=InputFile(io.BytesIO(file_content), filename="有效订阅.txt"),
                caption=f"来自用户 {user.id} 的有效订阅"
            )
        else:
            if not valid_results: return

            details_text = "\n\n".join([r['summary_text'] for r in valid_results])
            final_message = header_info + "\n<b>有效订阅详情:</b>\n" + details_text

            if len(final_message) > 4096:
                cutoff = 4096 - 100
                final_message = final_message[:cutoff] + "\n\n...(消息过长，已截断)"

            await bot.send_message(
                chat_id=NOTIFICATION_ID,
                text=final_message,
                parse_mode="HTML",
                disable_web_page_preview=True
            )

    except Exception as e:
        print(f"发送通知到管理员失败 (ID: {NOTIFICATION_ID}): {e}")
        try:
            await bot.send_message(chat_id=NOTIFICATION_ID,
                                   text=f"处理来自用户 {update.effective_user.id} 的查询并向您发送详细通知时出错: {e}")
        except Exception as fallback_e:
            print(f"发送回退通知失败: {fallback_e}")


def format_time_remaining(seconds: int) -> str:
    if seconds is None or seconds < 0:
        return "未知"
    d, remainder = divmod(seconds, 86400)
    h, remainder = divmod(remainder, 3600)
    m, _ = divmod(remainder, 60)
    return f"{int(d)}天{int(h)}小时{int(m)}分钟"


def format_traffic(b: int) -> str:
    if b is None: return "N/A"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while b >= 1024 and i < len(units) - 1:
        b /= 1024
        i += 1
    return f"{b:.2f}{units[i]}"


def gen_bar(pct: float, length: int = 12) -> str:
    full = int(pct / 100 * length)
    return f"[{'⬢' * full}{'⬡' * (length - full)}]"


def _format_progress_bar(current: int, total: int) -> str:
    if total == 0:
        return "查询/解析中... [⬡⬡⬡⬡⬡⬡⬡⬡⬡⬡] 0% (0/0)"

    percentage = (current / total) * 100
    bar_length = 12
    filled_length = int(bar_length * current // total)
    bar = '⬢' * filled_length + '⬡' * (bar_length - filled_length)
    return f"查询/解析中... [{bar}] {percentage:.0f}% ({current}/{total})"


def get_timestamped_urls(cache_file: str, new_urls: list[str]) -> list[str]:
    existing_urls_map = {}
    try:
        if os.path.exists(cache_file):
            with open(cache_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line: continue
                    match = re.search(r'\]\s*(https?://.*)', line)
                    if match:
                        url = match.group(1).strip()
                        existing_urls_map[url] = line
                    else:
                        existing_urls_map[line] = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {line}"
    except IOError as e:
        print(f"读取缓存文件时出错 {cache_file}: {e}")

    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for url in new_urls:
        url = url.strip()
        if url: existing_urls_map[url] = f"[{current_timestamp}] {url}"
    return sorted(existing_urls_map.values(), key=lambda item: item.split('] ')[-1])


def update_cache_file(new_urls: list[str]):
    all_timestamped_lines = get_timestamped_urls(CACHE_FILE, new_urls)
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            for line in all_timestamped_lines:
                f.write(line + '\n')
    except IOError as e:
        print(f"写入缓存文件时出错 {CACHE_FILE}: {e}")


def update_invalid_cache_file(new_urls: list[str]):
    all_timestamped_lines = get_timestamped_urls(INVALID_CACHE_FILE, new_urls)
    try:
        with open(INVALID_CACHE_FILE, 'w', encoding='utf-8') as f:
            for line in all_timestamped_lines:
                f.write(line + '\n')
    except IOError as e:
        print(f"写入无效缓存文件时出错 {INVALID_CACHE_FILE}: {e}")


def update_valid_nodes_file(new_nodes: list[str]):
    existing_nodes = set()
    try:
        if os.path.exists(VALID_NODES_FILE):
            with open(VALID_NODES_FILE, 'r', encoding='utf-8') as f:
                existing_nodes.update(line.strip() for line in f if line.strip())
    except IOError as e:
        print(f"读取有效节点文件时出错 {VALID_NODES_FILE}: {e}")

    existing_nodes.update(node.strip() for node in new_nodes if node.strip())
    try:
        with open(VALID_NODES_FILE, 'w', encoding='utf-8') as f:
            for node in sorted(list(existing_nodes)):
                f.write(node + '\n')
    except IOError as e:
        print(f"写入有效节点文件时出错 {VALID_NODES_FILE}: {e}")


async def try_get_filename_from_header_async(session: ClientSession, download_url: str) -> Optional[str]:
    headers_browser = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(download_url, timeout=10, headers=headers_browser) as r:
            if r.status == 200:
                cd = r.headers.get('Content-Disposition')
                if cd:
                    m1 = RE_FILENAME_UTF8.search(cd)
                    if m1: return unquote(m1.group(1)).replace('%20', ' ').replace('%2B', '+')
                    m2 = RE_FILENAME_SIMPLE.search(cd)
                    if m2: return unquote(m2.group(1)).replace('%20', ' ').replace('%2B', '+')
    except Exception:
        pass
    return None


async def try_get_title_from_html_async(session: ClientSession, base_url: str) -> str:
    headers_browser = {"User-Agent": "Mozilla/5.0"}
    try:
        content = None
        async with session.get(base_url + "/auth/login", headers=headers_browser, timeout=10) as resp:
            if resp.status != 200:
                async with session.get(base_url, headers=headers_browser, timeout=5) as resp_main:
                    content = await resp_main.read()
            else:
                content = await resp.read()

        soup = BeautifulSoup(content, "html.parser")
        title = soup.title.string.strip() if soup.title and soup.title.string else '未知'

        if "Cloudflare" in title: return '该域名仅限国内IP访问'
        if "Access denied" in title or "404" in title: return '该域名非机场面板域名'
        if "Just a moment" in title: return '该域名开启了5s盾'
        return title.replace("登录 — ", "")
    except Exception:
        return '未知'


async def get_filename_from_url_async(session: ClientSession, url: str) -> str:
    if "sub?target=" in url:
        inner_match = re.search(r"url=([^&]*)", url)
        if inner_match:
            return await get_filename_from_url_async(session, unquote(inner_match.group(1)))

    if "api/v1/client/subscribe?token" in url:
        if "&flag=clash" not in url: url += "&flag=clash"
        name = await try_get_filename_from_header_async(session, url)
        return name if name else '未知'
    try:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.hostname}"
        return await try_get_title_from_html_async(session, domain)
    except Exception:
        return '未知'


def extract_node_name(proxy: dict) -> str:
    for k in ["name", "ps", "desc", "remarks", "remark"]:
        if isinstance(proxy, dict) and k in proxy and proxy[k]:
            return str(proxy[k])
    return ""


def extract_quota_and_expire(node_name: str):
    quota_pat = re.compile(r'([\d\.]+ ?[MGTP]B?) ?\| ?([\d\.]+ ?[MGTP]B?)', re.I)
    expire_pats = [
        re.compile(r'Expire Date[:： ]+(\d{4}/\d{2}/\d{2})', re.I),
        re.compile(r'到期[日|时间|日期|至][:： ]*(\d{4}[-/]\d{2}[-/]\d{2})', re.I),
        re.compile(r'(\d{4}[-/]\d{2}[-/]\d{2})', re.I),
    ]
    quota, expire = None, None
    m = quota_pat.search(node_name)
    if m: quota = f"{m.group(1)} / {m.group(2)}"
    for pat in expire_pats:
        m = pat.search(node_name)
        if m:
            expire = m.group(1)
            break
    return quota, expire


def scan_proxies_quota_expire(proxies):
    quotas, expires = set(), set()
    for p in proxies:
        name = p.get('name', '') if isinstance(p, dict) else str(p)
        quota, expire = extract_quota_and_expire(name)
        if quota: quotas.add(quota)
        if expire: expires.add(expire)
    return list(quotas), list(expires)


def try_yaml_parse(text_content):
    text_content = text_content.lstrip('\ufeff').strip()
    if not text_content or ('proxies:' not in text_content and 'proxy-providers:' not in text_content):
        return []
    if _HAS_RUAMEL:
        try:
            from io import StringIO
            return list(yaml_ruamel.load_all(StringIO(text_content)))
        except Exception:
            pass
    try:
        return list(yaml.safe_load_all(text_content))
    except Exception:
        return []


def is_base64_string(s):
    return bool(re.fullmatch(r'[A-Za-z0-9+/=_-]{16,}', s))


def safe_b64decode(s):
    s = s.strip().replace('\r', '').replace('\n', '')
    if not is_base64_string(s): return None
    padding = len(s) % 4
    if padding != 0: s += "=" * (4 - padding)
    try:
        return base64.urlsafe_b64decode(s.encode()).decode('utf-8', errors='ignore')
    except (binascii.Error, ValueError):
        return None


def find_proxies_in_config(data):
    if isinstance(data, dict):
        if 'proxies' in data and isinstance(data['proxies'], list):
            return data['proxies']
        for value in data.values():
            found_proxies = find_proxies_in_config(value)
            if found_proxies is not None:
                return found_proxies
    return None


def parse_node_lines_with_b64(lines_to_parse, node_info, max_depth=3, current_depth=0):
    if current_depth > max_depth: return
    for line in lines_to_parse:
        stripped_line = line.strip()
        if not stripped_line: continue
        if stripped_line.lower().startswith(('http://', 'https://')):
            try:
                parsed_u = urlparse(stripped_line)
                if (parsed_u.path and parsed_u.path != '/') or parsed_u.query: continue
            except Exception:
                continue

        if len(stripped_line) > 20 and not RE_NODE_LINKS.match(stripped_line):
            decoded = safe_b64decode(stripped_line)
            if decoded:
                parse_node_lines_with_b64(decoded.splitlines(), node_info, max_depth, current_depth + 1)
                continue

        if RE_NODE_LINKS.match(stripped_line):
            node_info["node_count"] += 1
            if "all_node_links" in node_info: node_info["all_node_links"].append(stripped_line)
            protocol_type = stripped_line.split("://", 1)[0].upper()
            node_info["protocol_types"].add(protocol_type)
            
            node_name = ""
            try:
                name_match = re.search(r'#(.*)', stripped_line)
                if name_match:
                    node_name = unquote(name_match.group(1), 'utf-8', 'ignore').replace('+', ' ')
                else:
                    if protocol_type == 'VMESS':
                        base64_part = stripped_line.split('://', 1)[1]
                        base64_part = base64_part.split('?', 1)[0]
                        decoded_json_str = safe_b64decode(base64_part)
                        if decoded_json_str:
                            try:
                                vmess_config = json.loads(decoded_json_str)
                                node_name = vmess_config.get('ps', '')
                            except (json.JSONDecodeError, TypeError):
                                pass

                    if not node_name:
                        qs = urlparse(stripped_line).query
                        if qs:
                            params = dict(parse_qsl(qs))
                            node_name = params.get('remark', '') or params.get('name', '')
            except Exception:
                node_name = "无法解析名称"

            final_node_name = node_name or "无法解析名称"
            node_info["all_nodes_list"].append({"name": final_node_name, "protocol": protocol_type})
            country = SubscriptionBot._extract_country_from_name_static(final_node_name)
            if country != "未知": node_info["countries"].add(country)


class SubscriptionBot:
    def __init__(self):
        self.domain_map = {}
        self._sem = asyncio.Semaphore(CONCURRENT_LIMIT)
        self.load_domain_map()
        self._name_cache = {}
        self._node_info_cache = {}

    def load_domain_map(self):
        self.domain_map = {}
        try:
            if not os.path.exists(DOMAIN_MAP_FILE):
                with open(DOMAIN_MAP_FILE, "w", encoding="utf-8") as f: pass
                return
            with open(DOMAIN_MAP_FILE, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#') or "=" not in line: continue
                    try:
                        d, n = line.split("=", 1)
                        if d.strip() and n.strip(): self.domain_map[d.strip()] = n.strip()
                    except ValueError:
                        continue
        except Exception as e:
            print(f"加载域名映射文件时发生错误: {e}")

    def save_domain_map(self):
        try:
            with open(DOMAIN_MAP_FILE, "w", encoding="utf-8") as f:
                for d, n in self.domain_map.items():
                    f.write(f"{d}={n}\n")
        except IOError as e:
            print(f"写入域名映射文件时发生I/O错误: {e}")

    async def fetch_airport_name_from_response_header(self, session: ClientSession, url: str) -> str:
        parsed_url = urlparse(url)
        query_params = dict(parse_qsl(parsed_url.query))
        query_params['flag'] = 'clash'
        new_query = urlencode(query_params)
        url = urlunparse((parsed_url.scheme, parsed_url.netloc, parsed_url.path, parsed_url.params, new_query, parsed_url.fragment))
        try:
            async with session.get(url, timeout=10) as r:
                if r.status == 200:
                    cd = r.headers.get('Content-Disposition')
                    if cd:
                        m1 = RE_FILENAME_UTF8.search(cd)
                        if m1: return unquote(m1.group(1)).rsplit('.', 1)[0]
                        m2 = RE_FILENAME_SIMPLE.search(cd)
                        if m2: return unquote(m2.group(1)).rsplit('.', 1)[0]
                return '未识别的文件名格式'
        except Exception:
            return '请求异常'

    async def extract_name(self, session: ClientSession, url: str) -> str:
        if url in self._name_cache: return self._name_cache[url]
        host = urlparse(url).hostname or ""
        for key, val in self.domain_map.items():
            if key in host:
                self._name_cache[url] = val
                return val
        name = await self.fetch_airport_name_from_response_header(session, url)
        if not name.startswith(("请求", "未找到", "未识别")):
            self._name_cache[url] = name
            return name
        name = await get_filename_from_url_async(session, url)
        self._name_cache[url] = name
        return name

    @staticmethod
    def _extract_country_from_name_static(node_name: str) -> str:
        upper_name = node_name.upper()
        for country, keywords in COUNTRY_KEYWORDS.items():
            for keyword in keywords:
                if keyword.upper() in upper_name: return country
        return "未知"

    def _extract_country_from_name(self, node_name: str) -> str:
        return SubscriptionBot._extract_country_from_name_static(node_name)

    def _parse_node_lines(self, lines_to_parse: list[str], node_info: dict) -> None:
        parse_node_lines_with_b64(lines_to_parse, node_info)

    async def fetch_url_data(self, session: ClientSession, url: str, content_override: str = None):
        # --- 修改开始：增加带TTL的内存缓存逻辑 ---
        # 检查内存缓存，content_override不为None时则跳过缓存
        if content_override is None and url in self._node_info_cache:
            cache_time, cached_data = self._node_info_cache[url]
            # 检查缓存是否在30分钟有效期内
            if (time.time() - cache_time) < CACHE_TTL_SECONDS:
                return cached_data
            else:
                # 缓存已过期，将其删除
                del self._node_info_cache[url]
        # --- 修改结束 ---

        node_info = {"node_count": 0, "countries": set(), "protocol_types": set(), "all_nodes_list": [], "all_node_links": []}
        header_info = None
        text_content_to_parse = ""

        async with self._sem:
            if content_override:
                text_content_to_parse = content_override
            elif not url.lower().startswith(('http://', 'https://')):
                text_content_to_parse = url
            else:
                for i in range(3):
                    try:
                        req_url = self._append_timestamp(url)
                        headers = {"User-Agent": random.choice(CLIENT_USER_AGENTS)}
                        response = await fetch(req_url, { headers: { "User-Agent": "Mozilla/5.0" }, method: "GET", })
                            if resp.status == 200:
                                # --- 流量头信息解析 ---
                                header_info_str = resp.headers.get("subscription-userinfo")
                                if header_info_str:
                                    try:
                                        kv = dict(RE_USERINFO_KV.findall(header_info_str))
                                        header_info = {
                                            "upload": int(kv.get("upload", 0)),
                                            "download": int(kv.get("download", 0)),
                                            "total": int(kv.get("total", 0)),
                                            "expire": int(kv.get("expire", 0)) or None,
                                        }
                                        header_info["remaining_bytes"] = max(header_info["total"] - (header_info["upload"] + header_info["download"]), 0)
                                        header_info["has_exp"] = bool(header_info["expire"])
                                        header_info["remaining_secs"] = header_info["expire"] - int(time.time()) if header_info["expire"] else None
                                    except Exception:
                                        header_info = None
                                
                                # --- 节点内容解析 ---
                                raw_content = await resp.read()
                                text_content_to_parse = raw_content.decode('utf-8', 'ignore')
                                break # 成功获取，跳出重试循环
                    except (asyncio.TimeoutError, ClientError):
                        if i < 2: await asyncio.sleep(0.5 * (i + 1))
            
            if text_content_to_parse:
                parsed_successfully = False
                try:
                    all_docs = try_yaml_parse(text_content_to_parse)
                    for config in all_docs:
                        proxies_list = find_proxies_in_config(config)
                        if proxies_list:
                            for proxy in proxies_list:
                                node_info["node_count"] += 1
                                protocol_type = proxy.get('type', 'UNKNOWN').upper()
                                node_info["protocol_types"].add(protocol_type)
                                node_name = extract_node_name(proxy)
                                node_info["all_nodes_list"].append({"name": node_name, "protocol": protocol_type})
                                country = self._extract_country_from_name(node_name)
                                if country != "未知": node_info["countries"].add(country)
                            parsed_successfully = True
                            break
                except Exception:
                    pass

                if not parsed_successfully:
                    decoded_content = safe_b64decode(text_content_to_parse)
                    lines_to_parse = decoded_content.splitlines() if decoded_content else text_content_to_parse.splitlines()
                    self._parse_node_lines(lines_to_parse, node_info)

        # 准备最终的返回结果
        result_node_data = {
            "node_count": node_info["node_count"],
            "country_count": len(node_info["countries"]),
            "countries": sorted(list(node_info["countries"]), key=lambda x: ORDERED_COUNTRIES.index(x) if x in ORDERED_COUNTRIES else len(ORDERED_COUNTRIES)),
            "protocol_types": sorted(list(node_info["protocol_types"])),
            "all_nodes_list": node_info["all_nodes_list"],
            "all_node_links": node_info["all_node_links"]
        }
        
        # 将节点和流量信息打包成一个对象
        final_result = {"node_data": result_node_data, "header_info": header_info}
        
        # --- 修改开始：存入缓存时，同时保存当前时间戳 ---
        if content_override is None:
            self._node_info_cache[url] = (time.time(), final_result)
        # --- 修改结束 ---
            
        return final_result

    def _append_timestamp(self, url: str) -> str:
        ts = int(time.time() * 1000)
        return f"{url}&_={ts}" if "?" in url else f"{url}?_={ts}"


async def process_and_format_url(session: ClientSession, bot_inst: 'SubscriptionBot', url: str,
                                 original_filename: Optional[str] = None, content_override: Optional[str] = None):
    # 只调用一次统一的获取函数
    full_data = await bot_inst.fetch_url_data(session, url, content_override=content_override)
    
    node_data = full_data.get("node_data", {})
    info = full_data.get("header_info") # 直接从结果中获取流量信息

    node_count = node_data.get("node_count", 0)
    status = {"valid": False, "exhausted": False, "expired": False, "source_type": "url"}
    if url.startswith("file_content_direct://"): status["source_type"] = "file"
    if node_count > 0: status["valid"] = True
    else:
        return {"url": url, "summary_text": "", "detailed_text": "", "status": status, "all_nodes_list": [],
                "all_node_links": [], "original_filename": original_filename}

    name = '未知机场'
    if status["source_type"] == "file" and original_filename:
        name = original_filename.rsplit('.', 1)[0]
    else:
        name = await bot_inst.extract_name(session, url)

    country_count = node_data.get("country_count", 0)
    protocol_types = ",".join([x.lower() for x in node_data.get("protocol_types", [])]) or "未知"
    countries_list = ",".join(node_data.get("countries", [])) or "未知"
    safe_name, safe_url, safe_filename_str = safe_html(name), safe_html(url), safe_html(original_filename or '上传文件')

    main_part_template = f"机场名称: {safe_name}\n"
    if status["source_type"] == "url":
        if url.lower().startswith(('http://', 'https://')):
            main_part_template += f'订阅链接: <a href="{safe_url}">{safe_url}</a>\n'
        else:
            main_part_template = f"节点详情:\n"
    else:
        main_part_template += f"文件来源: {safe_filename_str}\n"

    # 这里的判断逻辑不变，但 `info` 的来源更可靠了
    if info and info.get('total', 0) > 0:
        used_bytes = info["upload"] + info["download"]
        used_pct = (used_bytes / info["total"] * 100) if info["total"] > 0 else 0
        remaining_bytes = info["remaining_bytes"]
        if remaining_bytes <= 1024 * 1024: status['exhausted'] = True
        exp_time = "长期有效"
        if info.get('has_exp') and info.get('expire'):
            if info['remaining_secs'] < 0:
                status['expired'], exp_time = True, "已过期"
            elif info['remaining_secs'] < 365 * 10 * 86400:
                expire_dt = datetime.fromtimestamp(info["expire"])
                exp_time = f"{expire_dt.strftime('%Y-%m-%d')} (剩{info['remaining_secs'] // 86400}天)"
        main_part_dynamic = (
            f"流量详情: {format_traffic(used_bytes)} / {format_traffic(info['total'])}\n"
            f"使用进度: {gen_bar(used_pct)} {used_pct:.1f}%\n"
            f"剩余可用: {format_traffic(remaining_bytes)}\n"
            f"过期时间: {safe_html(exp_time)}"
        )
    else:
        proxies_to_scan = node_data.get("all_nodes_list", [])
        quotas, expires = scan_proxies_quota_expire(proxies_to_scan)
        main_part_dynamic = (
            f"流量详情: {safe_html(' / '.join(quotas) or '未知')}\n"
            f"过期时间: {safe_html('、'.join(expires) or '长期有效')}"
        )

    all_nodes_list = node_data.get("all_nodes_list", [])
    total_nodes = len(all_nodes_list)
    if not url.lower().startswith(('http://', 'https://')) and total_nodes == 1:
        summary_nodes_info = (f"节点名称: {safe_html(all_nodes_list[0]['name'])}\n"
                              f"协议类型: {safe_html(protocol_types)}")
    else:
        summary_nodes_info = (f"协议类型: {safe_html(protocol_types)}\n"
                              f"节点总数: {node_count} | 国家/地区: {country_count}\n"
                              f"节点范围: {safe_html(countries_list)}")
    
    nodes_text_formatted = ""
    if all_nodes_list and not (not url.lower().startswith(('http://', 'https://')) and total_nodes == 1):
        formatted_nodes = [f"- {safe_html(node['protocol'].lower())}: {safe_html(node['name'])}" for node in all_nodes_list[:NODE_DISPLAY_LIMIT]]
        nodes_text_formatted = "\n".join(formatted_nodes)
        if total_nodes > NODE_DISPLAY_LIMIT:
            nodes_text_formatted += f"\n... 等等 ({total_nodes - NODE_DISPLAY_LIMIT} 个更多节点未显示)"

    summary_text = main_part_template + main_part_dynamic + f"\n<pre><code>{summary_nodes_info}</code></pre>"
    detailed_text = summary_text
    if nodes_text_formatted:
        detailed_text += f"\n<pre><code>{nodes_text_formatted}</code></pre>"

    if status.get('exhausted') or status.get('expired'):
        summary_text, detailed_text = f"<del>{summary_text}</del>", f"<del>{detailed_text}</del>"
    return {"url": url, "summary_text": summary_text, "detailed_text": detailed_text, "status": status,
            "all_nodes_list": all_nodes_list, "all_node_links": node_data.get("all_node_links", []),
            "original_filename": original_filename}


async def _extract_sources_from_message(message: Message) -> tuple[list, Optional[str], Optional[str], list]:
    urls, source_filename, text_content, initial_messages = [], None, None, []
    if message.document and message.document.file_name:
        original_file_name = message.document.file_name
        file_ext = os.path.splitext(original_file_name)[1].lower()
        if file_ext in ('.txt', '.yaml', '.yml'):
            try:
                file = await message.document.get_file()
                file_text = (await file.download_as_bytearray()).decode('utf-8', 'ignore')
                lines = [line.strip() for line in file_text.splitlines() if line.strip()]
                is_profile = any((
                    bool(try_yaml_parse(file_text)),
                    len(lines) == 1 and is_base64_string(lines[0]),
                    any(RE_DIRECT_NODE_LINKS.match(line) for line in lines)
                ))
                if is_profile:
                    urls.append(f"file_content_direct://{uuid.uuid4().hex}")
                    source_filename, text_content = original_file_name, file_text
                else:
                    extracted_urls = RE_URLS.findall(file_text)
                    if extracted_urls: urls.extend(extracted_urls)
                    else:
                        urls.append(f"file_content_direct://{uuid.uuid4().hex}")
                        source_filename, text_content = original_file_name, file_text
                return urls, source_filename, text_content, initial_messages
            except Exception as e:
                initial_messages.append(f"无法处理文件: <code>{safe_html(original_file_name)}</code>")
    if message.text:
        found_links = RE_URLS.findall(message.text)
        if found_links:
            unique_urls = sorted(list(dict.fromkeys(found_links)))
            if len(unique_urls) > TEXT_MESSAGE_URL_LIMIT:
                initial_messages.append(f"链接过多，仅处理前 {TEXT_MESSAGE_URL_LIMIT} 条。")
                urls.extend(unique_urls[:TEXT_MESSAGE_URL_LIMIT])
            else:
                urls.extend(unique_urls)
    return urls, source_filename, text_content, initial_messages


async def _process_and_get_results(update: Update, ctx: ContextTypes.DEFAULT_TYPE, urls_to_process: list[str],
                                   source_filename: Optional[str], text_content: Optional[str],
                                   processing_message: Optional[Message], initial_prefix_text: str):
    bot_inst: 'SubscriptionBot' = ctx.bot_data["bot"]
    timeout = ClientTimeout(total=30)
    results = []
    total_to_process = len(urls_to_process)
    last_update_time = time.time()

    async with ClientSession(timeout=timeout) as session:
        tasks = [asyncio.create_task(process_and_format_url(
            session, bot_inst, url,
            original_filename=source_filename,
            content_override=text_content if url.startswith("file_content_direct://") else None
        )) for url in urls_to_process]
        for i, future in enumerate(asyncio.as_completed(tasks)):
            try:
                results.append(await future)
            except Exception as e:
                print(f"处理URL时发生严重错误: {e}")
                results.append(None)
            
            current_time = time.time()
            if processing_message and total_to_process > 5 and (current_time - last_update_time > 0.5 or (i + 1) == total_to_process):
                try:
                    await processing_message.edit_text(initial_prefix_text + _format_progress_bar(i + 1, total_to_process), parse_mode="HTML")
                    last_update_time = current_time
                except BadRequest: pass
    return results


async def _format_and_reply(update: Update, ctx: ContextTypes.DEFAULT_TYPE, results: list,
                            urls_to_process: list, source_filename: Optional[str], text_content: Optional[str],
                            processing_message: Optional[Message]):
    if processing_message:
        try:
            await processing_message.delete()
        except BadRequest:
            pass

    valid_urls, invalid_urls, node_links = [], [], []
    for r in results:
        if isinstance(r, dict):
            status, url = r.get('status', {}), r.get('url')
            if status.get('source_type') == 'url' and url and url.lower().startswith(('http', 'https')):
                (valid_urls if status.get('valid') and not status.get('exhausted') and not status.get('expired') else invalid_urls).append(url)
            if r.get('all_node_links'): node_links.extend(r.get('all_node_links'))
    if valid_urls: update_cache_file(valid_urls)
    if invalid_urls: update_invalid_cache_file(invalid_urls)
    if node_links: update_valid_nodes_file(list(set(node_links)))

    valid_results, invalid_results = [], []
    for r in results:
        if r and r.get('status', {}).get('valid'):
            (valid_results if not r.get('status',{}).get('exhausted') and not r.get('status',{}).get('expired') else invalid_results).append(r)

    total, valid_c, invalid_c = len(urls_to_process), len(valid_results), len(invalid_results)
    exhausted_c = sum(1 for r in invalid_results if r.get('status',{}).get('exhausted'))
    expired_c = sum(1 for r in invalid_results if r.get('status',{}).get('expired'))
    failed_c = total - (valid_c + invalid_c)
    stats_line = ""
    if total > 1:
        stats_line = f"\n\n查询统计: 有效: {valid_c} | 耗尽: {exhausted_c} | 过期: {expired_c} | 失效: {failed_c}"

    if total > FILE_OUTPUT_THRESHOLD:
        await update.message.reply_text(f"检测到 {total} 条链接，超过阈值，结果将以文件形式发送。" + stats_line, parse_mode="HTML")
        
        def clean_html(html_text: str) -> str:
            # 1. 先将 <pre><code> 这种特殊结构转换成换行
            text_with_newlines = html_text.replace('<pre><code>', '\n').replace('</code></pre>', '')
            # 2. 使用通用的正则表达式，移除所有 <...> 形式的HTML标签
            plain_text = re.sub(r'<[^>]+>', '', text_with_newlines)
            # 3. 处理像 &amp; 这样的HTML实体编码，并去除首尾多余的空白
            return html.unescape(plain_text).strip()

        if valid_results:
            await update.message.reply_document(document=InputFile(io.BytesIO("\n\n".join(clean_html(r['summary_text']) for r in valid_results).encode('utf-8')), filename="有效订阅.txt"))
        if invalid_results:
            await update.message.reply_document(document=InputFile(io.BytesIO("\n\n".join(clean_html(r['summary_text']) for r in invalid_results).encode('utf-8')), filename="无效订阅.txt"))
        return

    # --- 修改点 #2：自动删除无法解析的提示 ---
    if not valid_results and not invalid_results:
        msg_to_delete = await update.message.reply_text("亲~ 您发送的内容无法解析，请检查后重试。")
        await asyncio.sleep(2)
        try:
            await msg_to_delete.delete()
        except (BadRequest, TimedOut, NetworkError):
            pass
        return
    # --- 修改点结束 ---

    message_parts = []
    if valid_results: message_parts.append("\n\n".join(r['summary_text'] for r in valid_results))
    if invalid_results: message_parts.append("\n\n".join(r['summary_text'] for r in invalid_results))
    summary_text = "\n\n".join(message_parts) + stats_line
    if failed_c > 0 and total == 1: summary_text += "\n\nℹ️ 该链接/文件无法处理或解析失败。"

    if len(summary_text) > 4096:
        await update.message.reply_text("结果内容过长，已转为文件发送。", document=InputFile(io.BytesIO(re.sub('<[^<]+?>', '', summary_text).encode('utf-8')), filename="查询结果.txt"))
    else:
        job_id = uuid.uuid4().hex
        all_results = valid_results + invalid_results
        detailed_text = "\n\n".join(r['detailed_text'] for r in all_results) + stats_line
        ctx.bot_data["refresh_jobs"][job_id] = {
            "tasks": [{'type': 'file' if r.get('status',{}).get('source_type') == 'file' else 'url', 'url': r['url'], 'filename': r.get('original_filename'), 'content': text_content if r.get('status',{}).get('source_type') == 'file' else None} for r in all_results],
            "summary_view_text": summary_text, "detailed_view_text": detailed_text, "current_view_is_detailed": False
        }
        keyboard = [[InlineKeyboardButton("刷新", callback_data=f"refresh:{job_id}"), InlineKeyboardButton("显示节点", callback_data=f"show_nodes:{job_id}")]]
        await update.message.reply_text(summary_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard), disable_web_page_preview=True)


async def handle_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message: return
    urls, source_filename, text_content, initial_messages = await _extract_sources_from_message(message)
    all_urls = sorted(list(dict.fromkeys(urls)))
    if not all_urls: return
    
    if len(all_urls) > QUERY_URL_LIMIT:
        initial_messages.append(f"总链接数超过 {QUERY_URL_LIMIT}，已截断。")
        all_urls = all_urls[:QUERY_URL_LIMIT]

    # --- 修改点 #1：优化提示逻辑 ---
    num_to_process = len(all_urls)
    initial_prefix = "\n".join(initial_messages)
    
    status_text = f"检测到 {num_to_process} 个链接/文件，正在为您查询……"
    full_initial_text = (initial_prefix + "\n\n" + status_text) if initial_prefix else status_text
    processing_msg = await message.reply_text(full_initial_text, parse_mode="HTML")

    initial_prefix_for_progress = (initial_prefix + "\n\n" if initial_prefix else "")
    if num_to_process > 5:
        progress_bar_text = _format_progress_bar(0, num_to_process)
        try:
            await processing_msg.edit_text(initial_prefix_for_progress + progress_bar_text, parse_mode="HTML")
        except BadRequest:
            pass
    # --- 修改点结束 ---

    results = await _process_and_get_results(update, ctx, all_urls, source_filename, text_content, processing_msg, initial_prefix_for_progress)
    
    await _format_and_reply(update, ctx, results, all_urls, source_filename, text_content, processing_msg)
    
    if NOTIFICATION_ID:
        if any(r and r.get('status', {}).get('valid') and not r.get('status', {}).get('exhausted') and not r.get('status', {}).get('expired') for r in results):
            await send_notification_to_admin(ctx.bot, update, results)


async def refresh_button_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    bot_inst: SubscriptionBot = ctx.bot_data["bot"]
    try:
        job_id = query.data.split(":", 1)[1]
    except (IndexError, AttributeError):
        await query.edit_message_text("错误：无效的回调数据。", reply_markup=None)
        return
    job_data = ctx.bot_data.get("refresh_jobs", {}).get(job_id)
    if not job_data or "tasks" not in job_data:
        # --- 修改开始 ---
        msg_to_delete = await query.edit_message_text("抱歉，此刷新请求已过期。", reply_markup=None)
        await asyncio.sleep(2)
        try:
            await msg_to_delete.delete()
        except (BadRequest, TimedOut, NetworkError):
            pass
        return
        # --- 修改结束 ---
        
    for task in job_data["tasks"]:
        url = task['url']
        # 刷新时，主动从内存缓存中移除
        bot_inst._node_info_cache.pop(url, None)
        bot_inst._name_cache.pop(url, None)
        
    await query.answer(text="正在刷新，请稍候...")
    
    timeout = ClientTimeout(total=30)
    async with ClientSession(timeout=timeout) as session:
        tasks = [asyncio.create_task(process_and_format_url(session, bot_inst, task['url'], original_filename=task.get('filename'), content_override=task.get('content'))) for task in job_data["tasks"]]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
    valid_urls, invalid_urls, node_links = [], [], []
    for r in results:
        if isinstance(r, dict):
            status, url = r.get('status', {}), r.get('url')
            if status.get('source_type') == 'url' and url and url.lower().startswith(('http', 'https')):
                (valid_urls if status.get('valid') and not status.get('exhausted') and not status.get('expired') else invalid_urls).append(url)
            if r.get('all_node_links'): node_links.extend(r.get('all_node_links'))
    if valid_urls: update_cache_file(valid_urls)
    if invalid_urls: update_invalid_cache_file(invalid_urls)
    if node_links: update_valid_nodes_file(list(set(node_links)))

    valid_results, invalid_results = [], []
    for r in results:
        if r and isinstance(r, dict) and r.get('status', {}).get('valid'):
            (valid_results if not r.get('status',{}).get('exhausted') and not r.get('status',{}).get('expired') else invalid_results).append(r)

    total = len(job_data["tasks"])
    stats_line = ""
    if total > 1:
        stats_line = f"\n\n查询统计: 有效: {len(valid_results)} | 耗尽: {sum(1 for r in invalid_results if r.get('status',{}).get('exhausted'))} | 过期: {sum(1 for r in invalid_results if r.get('status',{}).get('expired'))} | 失效: {total - (len(valid_results) + len(invalid_results))}"

    message_parts = []
    if valid_results: message_parts.append("\n\n".join(r['summary_text'] for r in valid_results))
    if invalid_results: message_parts.append("\n\n".join(r['summary_text'] for r in invalid_results))
    summary_text = "\n\n".join(message_parts) + stats_line
    
    all_results = valid_results + invalid_results
    detailed_text = "\n\n".join(r['detailed_text'] for r in all_results) + stats_line
    
    job_data["summary_view_text"], job_data["detailed_view_text"] = summary_text, detailed_text
    
    new_text = detailed_text if job_data["current_view_is_detailed"] else summary_text
    button_text = "折叠节点" if job_data["current_view_is_detailed"] else "显示节点"
    callback_action = "hide_nodes" if job_data["current_view_is_detailed"] else "show_nodes"
    
    if not new_text.strip(): new_text = "刷新后未找到有效信息。"
    if len(new_text) > 4096:
        await query.answer(text="刷新后内容过长，无法显示。", show_alert=True)
        return
        
    keyboard = [[InlineKeyboardButton("刷新", callback_data=f"refresh:{job_id}"), InlineKeyboardButton(button_text, callback_data=f"{callback_action}:{job_id}")]]
    try:
        await query.edit_message_text(text=new_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard), disable_web_page_preview=True)
    except BadRequest as e:
        if 'Message is not modified' not in str(e): print(f"刷新时编辑消息失败: {e}")
        else: await query.answer(text="信息无变化。")


async def toggle_nodes_view_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        action, job_id = query.data.split(":", 1)
    except (ValueError, IndexError):
        await query.edit_message_text("错误：无效的回调数据。", reply_markup=None)
        return
    job_data = ctx.bot_data.get("refresh_jobs", {}).get(job_id)
    if not job_data:
        await query.edit_message_text("抱歉，此请求已过期。", reply_markup=None)
        return
        
    job_data["current_view_is_detailed"] = (action == "show_nodes")
    new_text = job_data["detailed_view_text"] if job_data["current_view_is_detailed"] else job_data["summary_view_text"]
    button_text = "折叠节点" if job_data["current_view_is_detailed"] else "显示节点"
    callback_action = "hide_nodes" if job_data["current_view_is_detailed"] else "show_nodes"

    if len(new_text) > 4096:
        await query.answer(text="内容过长，无法展开/折叠。", show_alert=True)
        return
        
    await query.answer()
    keyboard = [[InlineKeyboardButton("刷新", callback_data=f"refresh:{job_id}"), InlineKeyboardButton(button_text, callback_data=f"{callback_action}:{job_id}")]]
    try:
        await query.edit_message_text(text=new_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard), disable_web_page_preview=True)
    except BadRequest as e:
        if 'Message is not modified' not in str(e): print(f"切换视图时编辑消息失败: {e}")


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("订阅信息检查机器人已启动！\n发送订阅链接、节点链接或包含这些内容的 .txt/.yaml/.yml 文件即可查询。")


async def help_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "使用说明\n\n"
        "如何查询:\n"
        "直接向机器人发送以下任意内容：\n"
        "  • 订阅链接 (http://...)\n"
        "  • 包含订阅链接或节点链接的 <code>.txt</code> 文件\n"
        "  • Clash 配置文件 (<code>.yaml</code> / <code>.yml</code>)\n\n"
        "结果交互:\n"
        "  • 刷新: 重新获取并更新当前订阅的信息。\n"
        "  • 显示/折叠节点: 展开或收起详细的节点列表。\n\n"
        "管理员指令:\n"
        "<code>/zj domain=名称</code> - 添加域名映射\n"
        "<code>/zj</code> - 查看所有映射\n"
        "<code>/clear</code> - 清空所有缓存"
    )
    await update.message.reply_text(help_text, parse_mode="HTML")


async def ping_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    latency = int((time.time() - update.message.date.timestamp()) * 1000)
    await update.message.reply_text(f"Bot 在线，延迟约 {latency} ms")


async def zj_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    bot_inst: SubscriptionBot = ctx.bot_data["bot"]
    arg = update.message.text.partition(" ")[2].strip()
    if not arg:
        if not bot_inst.domain_map: return await update.message.reply_text("当前没有域名映射。")
        map_str = "\n".join(f"<code>{safe_html(d)}</code> → <code>{safe_html(n)}</code>" for d, n in bot_inst.domain_map.items())
        return await update.message.reply_text(f"当前域名映射：\n{map_str}", parse_mode="HTML")
    if "=" not in arg: return await update.message.reply_text("用法：<code>/zj domain=名称</code>", parse_mode="HTML")
    d, n = (s.strip() for s in arg.split("=", 1))
    bot_inst.domain_map[d] = n
    bot_inst.save_domain_map()
    bot_inst._name_cache.clear()
    await update.message.reply_text(f"已添加映射：<code>{safe_html(d)}</code> → <code>{safe_html(n)}</code>", parse_mode="HTML")


async def zh_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    sent_count = 0
    for caption, file_path in {"有效订阅": CACHE_FILE, "无效订阅": INVALID_CACHE_FILE, "有效节点": VALID_NODES_FILE}.items():
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            try:
                await ctx.bot.send_document(chat_id=update.effective_chat.id, document=open(file_path, "rb"), filename=os.path.basename(file_path), caption=f"缓存的{caption}文件。")
                sent_count += 1
            except Exception as e:
                await update.message.reply_text(f"发送文件 {file_path} 出错: {e}")
    if sent_count == 0: await update.message.reply_text("当前没有任何可导出的缓存文件。")


async def clear_command(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    msg = await update.message.reply_text("正在清理缓存...")
    # 清理文件缓存
    for file_path in [CACHE_FILE, INVALID_CACHE_FILE, VALID_NODES_FILE]:
        if os.path.exists(file_path): os.remove(file_path)
        open(file_path, 'w').close()
    if os.path.exists(TEMP_YAML_DIR): shutil.rmtree(TEMP_YAML_DIR)
    os.makedirs(TEMP_YAML_DIR, exist_ok=True)
    
    # 清理内存缓存
    bot_inst: SubscriptionBot = ctx.bot_data["bot"]
    bot_inst._name_cache.clear()
    bot_inst._node_info_cache.clear()
    ctx.bot_data["refresh_jobs"].clear()
    
    await msg.edit_text("清理完成。")


async def set_bot_commands(app):
    await app.bot.set_my_commands([
        BotCommand("start", "启动机器人"),
        BotCommand("help", "获取帮助说明"),
        BotCommand("ping", "测试网络延迟"),
        BotCommand("zj", "设置域名映射 (管理员)"),
        BotCommand("clear", "清空所有缓存 (管理员)"),
    ])


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        print("错误: 环境变量 TELEGRAM_BOT_TOKEN 未设置。")
        return

    os.makedirs(TEMP_YAML_DIR, exist_ok=True)
    for f in [CACHE_FILE, INVALID_CACHE_FILE, VALID_NODES_FILE, DOMAIN_MAP_FILE]:
        if not os.path.exists(f):
            try:
                with open(f, 'w', encoding='utf-8') as fp: pass
            except Exception as e:
                print(f"创建文件 {f} 失败: {e}")

    app = Application.builder().token(token).build()
    app.bot_data["bot"] = SubscriptionBot()
    app.bot_data["refresh_jobs"] = {}

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("ping", ping_command))
    app.add_handler(CommandHandler("zj", zj_command))
    app.add_handler(CommandHandler("zh", zh_command))
    app.add_handler(CommandHandler("clear", clear_command))
    app.add_handler(MessageHandler(filters.TEXT | filters.Document.ALL, handle_message))
    app.add_handler(CallbackQueryHandler(refresh_button_callback, pattern=r"^refresh:"))
    app.add_handler(CallbackQueryHandler(toggle_nodes_view_callback, pattern=r"^(show_nodes|hide_nodes):"))

    app.post_init = set_bot_commands
    print("Bot is running...")
    app.run_polling()


if __name__ == "__main__":
    main()