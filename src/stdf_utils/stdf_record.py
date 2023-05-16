import logging
import math
import sys
from typing import Dict, Tuple, Any
from .util import unp


class StdfRecord:
    def __init__(self, fp, parse_types: set = None):
        self.fp = fp
        self.record_table = self._get_record_table()
        self.parse_types: set = parse_types or {r["name"] for r in self.record_table.values()}
        self.endian = "@"
        # cache
        self._rec_type: str = ""
        self._rec_fields: Tuple[Tuple[str, str]] = tuple()
        self._data: Dict[str, Any] = {}

    def __iter__(self):
        while True:
            try:
                record = self._get_next_record()
                if record is not None:
                    yield self._rec_type, record

            except EOFError:
                logging.debug("Completed...")
                break

            except BufferError:
                logging.error("Incomplete log...")
                break

    def _get_next_record(self):
        if self.endian == "@":
            self.far_handler()
            return None  # do not return Far

        # reset
        self._rec_type = ""
        self._rec_fields = tuple()
        self._data = {}

        # read header (4 bytes)
        header = self.fp.read(4)
        if len(header) == 0:
            raise EOFError

        elif len(header) != 4:
            raise BufferError

        body = self.fp.read(unp(self.endian, 'H', header[0:2]))
        key: int = header[2] * 1000 + header[3]  # typ * 1000 + sub
        record = self.record_table.get(key)
        if record is None:
            logging.error(f"Unknown key: {key}")
            return None

        # parse type filter
        self._rec_type = record["name"]
        if record["name"] in self.parse_types:
            return self.parse(record, body)

    def far_handler(self):
        buf = self.fp.read(6)
        cpu_type = buf[4]
        if cpu_type == 1:
            self.endian = ">"
        elif cpu_type == 2:
            self.endian = "<"
        else:
            raise ValueError(f"Cpu type '{cpu_type}' is not supported...")

    def parse(self, record, body):
        self._rec_fields = record["fields"]
        for field_name, fmt in self._rec_fields:
            val, body = self._get_parse_func(fmt)(fmt, body)
            self._data[field_name] = val
        return self._data

    def _get_parse_func(self, fmt):
        if fmt in {'U4', 'U1', 'U2', 'U8'}:
            return self._get_Un
        elif fmt in {'I1', 'I2', 'I4'}:
            return self._get_In
        elif fmt in {'R4', 'R8'}:
            return self._get_Rn
        elif fmt == 'Cn' or (fmt.startswith('C') and (fmt[1:].isdigit())):
            return self._get_Cn
        elif fmt in {'B1', 'Bn', 'B0'}:
            return self._get_Bn
        elif fmt.startswith('K'):
            return self._get_Kx
        elif fmt == 'N1':
            return self._get_Nn
        elif fmt == 'Dn':
            return self._get_Dn
        elif fmt == 'Vn':
            return self._get_Vn
        else:
            assert False, 'Unknown Format: %s' % fmt

    def _get_Nn(self, fmt, buf):
        """ Note: this function process two N1 type every time instead of one """
        r = []
        if fmt == 'N1':
            if len(buf) < 1:
                return None, ''
            else:
                tmp = unp(self.endian, 'B', buf[0])
                r.append(tmp & 0x0F)
                r.append(tmp >> 4)
                return r, buf[1:]
        else:
            raise TypeError(f'_get_Nn: Error format: {fmt}')

    def _get_Un(self, fmt, buf):
        # logging.debug('In Get_Un(): %s' % format)
        if fmt == 'U4':
            if len(buf) < 4:
                return None, ''
            else:
                r = unp(self.endian, 'I', buf[0:4])  # I -> unsigned int
                return r, buf[4:]
        elif fmt == 'U2':
            if len(buf) < 2:
                return None, ''
            else:
                r = unp(self.endian, 'H', buf[0:2])  # H -> unsigned short
                return r, buf[2:]
        elif fmt == 'U1':
            if len(buf) < 1:
                return None, ''
            else:
                r = unp(self.endian, 'B', buf[0:1])  # B -> unsigned char
                return r, buf[1:]
        elif fmt == "U8":
            if len(buf) < 8:
                return None, ''
            r = unp(self.endian, 'Q', buf[0:8])  # Q -> unsigned long long
            return r, buf[8:]

        else:
            logging.critical('Error format: %s' % fmt)
            sys.exit(-1)

    def _get_In(self, fmt, buf):
        if fmt == 'I4':
            if len(buf) < 4:
                return None, ''
            else:
                r = unp(self.endian, 'i', buf[0:4])
                return r, buf[4:]
        elif fmt == 'I2':
            if len(buf) < 2:
                return None, ''
            else:
                r = unp(self.endian, 'h', buf[0:2])
                return r, buf[2:]
        elif fmt == 'I1':
            if len(buf) < 2:
                return None, ''
            else:
                r = unp(self.endian, 'b', buf[0:1])
                return r, buf[1:]
        else:
            logging.critical('Error format: %s' % fmt)
            sys.exit(-1)

    def _get_Rn(self, fmt, buf):
        #        logging.debug('In Get_Rn() %s' % format)
        if fmt == 'R4':
            if len(buf) < 4:
                return None, ''
            else:
                r = unp(self.endian, 'f', buf[0:4])
                return r, buf[4:]
        elif fmt == 'R8':
            if len(buf) < 8:
                return None, ''
            else:
                r = unp(self.endian, 'd', buf[0:8])
                return r, buf[8:]
        else:
            logging.critical('Error format: %s' % fmt)
            sys.exit(-1)

    @staticmethod
    def _get_Cn(fmt, buf):
        #        logging.debug('In Get_Cn(): %s' % format)
        if fmt == 'C1':
            if len(buf) < 1:
                return None, ''
            else:
                r = buf[0]
                return r, buf[1:]
        elif fmt == 'Cn':
            if len(buf) < 1:
                return None, ''
            else:
                char_cnt = buf[0]
                if len(buf) < (1 + char_cnt):
                    logging.critical(f'Cn: Not enough data in buffer: needed: {str(1 + char_cnt)},'
                                     f' actual: {str(len(buf))}')
                    return buf[1:], buf[len(buf):]
                r = buf[1:(1 + char_cnt)]
                return r, buf[(1 + char_cnt):]
        elif fmt.startswith('C') and fmt[1:].isdigit():
            if len(buf) < 1:
                return None, ''
            else:
                cnt = int(fmt[1:])
                r = buf[0:cnt]
                return r, buf[cnt:]
        else:
            logging.critical('Error format: %s' % fmt)
            sys.exit(-1)

    def _get_Bn(self, fmt, buf):
        hex_digits = '0123456789ABCDEF'
        if fmt in ['B1', 'B0']:
            if len(buf) < 1:
                return None, ''
            else:
                r = buf[0]
                r = '0x' + hex_digits[r >> 4] + hex_digits[r & 0x0F]
                if len(buf) == 1:
                    return r, ''
                else:
                    return r, buf[1:]
        if fmt == 'Bn':
            if len(buf) < 1:
                return None, ''
            else:
                char_cnt = unp(self.endian, 'B', bytes([buf[0]]))
                if len(buf) < (1 + char_cnt):
                    logging.critical('Bn: Not enough data in buffer: needed: %s, actual: %s' % (str(1 + char_cnt),
                                                                                                str(len(buf))))
                    sys.exit(-1)
                r = buf[1:(1 + char_cnt)]
                tmp = '0x'
                for i in r:
                    v = unp(self.endian, 'B', i)
                    tmp = tmp + hex_digits[v >> 4] + hex_digits[v & 0x0F]
                r = tmp
                return r, buf[(1 + char_cnt):]
        else:
            logging.critical('Error format: %s' % fmt)
            sys.exit(-1)

    def _get_Dn(self, fmt, buf):
        if fmt == 'Dn':
            if len(buf) < 1:
                return None, ''
            else:
                dlen = unp(self.endian, 'H', buf[0:2])
                buf = buf[2:]
                r = []
                dbyt = int(math.ceil(dlen / 8.0))
                assert len(buf) >= dbyt
                for i in range(dbyt):
                    tmp = unp(self.endian, 'B', buf[i])
                    for j in range(8):
                        r.append((tmp >> j) & 0x01)
                return r, buf[dbyt:]

    def _get_Kx(self, fmt, buf):
        # first, parse the format to find out in which field of the record defined the length of the array
        assert fmt.startswith('K'), f'In Get_Kx(): format error: {fmt}'
        assert len(fmt) == 4 or len(fmt) == 5
        # assume format = 'K3U4' or 'K13U4', then item_format = 'U4'
        item_format = fmt[-2:]
        # then index_cnt = 3 or 13
        index_cnt = int(fmt[1:-2])
        cnt_name = self._rec_fields[index_cnt][0]
        cnt = self._data[cnt_name]
        r = []
        func = self._get_parse_func(item_format)
        if item_format == 'N1':
            cnt = int(math.ceil(cnt / 2.0))
            for i in range(cnt):
                item, buf = func(item_format, buf)
                r.append(item[0])
                r.append(item[1])
            return r, buf
        else:
            for i in range(cnt):
                item, buf = func(item_format, buf)
                r.append(item)
            return r, buf

    def _get_Vn(self, fmt, buf):
        assert fmt == "Vn"
        data_type = ['B0', 'U1', 'U2', 'U4', 'I1', 'I2', 'I4',
                     'R4', 'R8', 'Cn', 'Bn', 'Dn', 'N1']
        if len(buf) < 1:
            return None, ''
        else:
            typ = buf[0]
            buf = buf[1:]
            r, buf = self._get_parse_func(data_type[typ])(data_type[typ], buf)
            return r, buf

    @staticmethod
    def _get_record_table() -> dict:
        return {
            10: {
                "name": "Far",  # (0, 10)
                "fields": (
                    ('CPU_TYPE', 'U1'),
                    ('STDF_VER', 'U1'),
                )
            },

            20: {
                "name": "Atr",  # (0, 20)
                "fields": (
                    ('MOD_TIM', 'U4'),
                    ('CMD_LINE', 'Cn')
                )
            },
            
            30: {
                "name": "Vur",  # (0, 30)
                "fields": (
                    ('UPD_CNT', 'U1'),
                    ('UPD_NAM', 'K0Cn'),
                    ('VUR_DUMMY', 'U1'),
                )
            },
            
            1010: {
                "name": "Mir",   # (1, 10)
                "fields": (
                    ('SETUP_T', 'U4'),
                    ('START_T', 'U4'),
                    ('STAT_NUM', 'U1'),
                    ('MODE_COD', 'C1'),
                    ('RTST_COD', 'C1'),
                    ('PROT_COD', 'C1'),
                    ('BURN_TIM', 'U2'),
                    ('CMOD_COD', 'C1'),
                    ('LOT_ID', 'Cn'),
                    ('PART_TYP', 'Cn'),
                    ('NODE_NAM', 'Cn'),
                    ('TSTR_TYP', 'Cn'),
                    ('JOB_NAM', 'Cn'),
                    ('JOB_REV', 'Cn'),
                    ('SBLOT_ID', 'Cn'),
                    ('OPER_NAM', 'Cn'),
                    ('EXEC_TYP', 'Cn'),
                    ('EXEC_VER', 'Cn'),
                    ('TEST_COD', 'Cn'),
                    ('TST_TEMP', 'Cn'),
                    ('USER_TXT', 'Cn'),
                    ('AUX_FILE', 'Cn'),
                    ('PKG_TYP', 'Cn'),
                    ('FAMLY_ID', 'Cn'),
                    ('DATE_COD', 'Cn'),
                    ('FACIL_ID', 'Cn'),
                    ('FLOOR_ID', 'Cn'),
                    ('PROC_ID', 'Cn'),
                    ('OPER_FRQ', 'Cn'),
                    ('SPEC_NAM', 'Cn'),
                    ('SPEC_VER', 'Cn'),
                    ('FLOW_ID', 'Cn'),
                    ('SETUP_ID', 'Cn'),
                    ('DSGN_REV', 'Cn'),
                    ('ENG_ID', 'Cn'),
                    ('ROM_COD', 'Cn'),
                    ('SERL_NUM', 'Cn'),
                    ('SUPR_NAM', 'Cn')
                )
            },
            
            1020: {
                "name": "Mrr",  # (1, 20),
                "fields": (
                    ('FINISH_T', 'U4'),
                    ('DISP_COD', 'C1'),
                    ('USR_DESC', 'Cn'),
                    ('EXC_DESC', 'Cn')
                )
            },
            
            1030: {
                "name": "Pcr",  # (1, 30)
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('PART_CNT', 'U4'),
                    ('RTST_CNT', 'U4'),
                    ('ABRT_CNT', 'U4'),
                    ('GOOD_CNT', 'U4'),
                    ('FUNC_CNT', 'U4')
                )
            },
            
            1040: {
                "name": "Hbr",
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('HBIN_NUM', 'U2'),
                    ('HBIN_CNT', 'U4'),
                    ('HBIN_PF', 'C1'),
                    ('HBIN_NAM', 'Cn')
                )
            },
            
            1050: {
                "name": "Sbr",
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('SBIN_NUM', 'U2'),
                    ('SBIN_CNT', 'U4'),
                    ('SBIN_PF', 'C1'),
                    ('SBIN_NAM', 'Cn')
                )
            },

            1060: {
                "name": "Pmr",  # (1, 60)
                "fields": (
                    ('PMR_INDX', 'U2'),
                    ('CHAN_TYP', 'U2'),
                    ('CHAN_NAM', 'Cn'),
                    ('PHY_NAM', 'Cn'),
                    ('LOG_NAM', 'Cn'),
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1')
                )
            }, 
            
            1062: {
                "name": "Pgr",  # (1, 62),
                "field_ma": (
                    ('GRP_INDX', 'U2'),
                    ('GRP_NAM', 'Cn'),
                    ('INDX_CNT', 'U2'),
                    ('PMR_INDX', 'K2U2')
                )
            },
            
            1063: {
                "name": "Plr",  # (1, 63)
                "fields": (
                    ('GRP_CNT', 'U2'),
                    ('GRP_INDX', 'K0U2'),
                    ('GRP_MODE', 'K0U2'),
                    ('GRP_RADX', 'K0U1'),
                    ('PGM_CHAR', 'K0Cn'),
                    ('RTN_CHAR', 'K0Cn'),
                    ('PGM_CHAL', 'K0Cn'),
                    ('RTN_CHAL', 'K0Cn')
                )
            },
            
            1070: {
                "name": "Rdr",  # (1, 70)
                "fields": (
                    ('NUM_BINS', 'U2'),
                    ('RTST_BIN', 'K0U2')
                )
            }, 
            
            1080: {
                "name": "Sdr",  # (1, 80)
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_GRP', 'U1'),
                    ('SITE_CNT', 'U1'),
                    ('SITE_NUM', 'K2U1'),
                    ('HAND_TYP', 'Cn'),
                    ('HAND_ID', 'Cn'),
                    ('CARD_TYP', 'Cn'),
                    ('CARD_ID', 'Cn'),
                    ('LOAD_TYP', 'Cn'),
                    ('LOAD_ID', 'Cn'),
                    ('DIB_TYP', 'Cn'),
                    ('DIB_ID', 'Cn'),
                    ('CABL_TYP', 'Cn'),
                    ('CABL_ID', 'Cn'),
                    ('CONT_TYP', 'Cn'),
                    ('CONT_ID', 'Cn'),
                    ('LASR_TYP', 'Cn'),
                    ('LASR_ID', 'Cn'),
                    ('EXTR_TYP', 'Cn'),
                    ('EXTR_ID', 'Cn')
                )
            },
            
            1090: {
                "name": "Psr",  # (10, 90)
                "fields": (
                    ('CONT_FLG', 'B1'),
                    ('PSR_INDX', 'U2'),
                    ('PSR_NAM', 'Cn'),
                    ('OPT_FLG', 'B1'),
                    ('TOTP_CNT', 'U2'),
                    ('LOCP_CNT', 'U2'),
                    ('PAT_BGN', 'K5U8'),
                    ('PAT_END', 'K5U8'),
                    ('PAT_FILE', 'K5Cn'),
                    ('PAT_LBL', 'K5Cn'),
                    ('FILE_UID', 'K5Cn'),
                    ('ATPG_DSC', 'K5Cn'),
                    ('SRC_ID', 'K5Cn')
                )
            },

            2010: {
                "name": "Wir",  # (2, 10)
                "fields":  (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_GRP', 'U1'),
                    ('START_T', 'U4'),
                    ('WAFER_ID', 'Cn')
                )
            }, 
            
            2020: {
                "name": "Wrr",  # (2, 20)
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_GRP', 'U1'),
                    ('FINISH_T', 'U4'),
                    ('PART_CNT', 'U4'),
                    ('RTST_CNT', 'U4'),
                    ('ABRT_CNT', 'U4'),
                    ('GOOD_CNT', 'U4'),
                    ('FUNC_CNT', 'U4'),
                    ('WAFER_ID', 'Cn'),
                    ('FABWF_ID', 'Cn'),
                    ('FRAME_ID', 'Cn'),
                    ('MASK_ID', 'Cn'),
                    ('USR_DESC', 'Cn'),
                    ('EXC_DESC', 'Cn')
                )
            }, 
            
            2030: {
                "name": "Wcr",  # (2, 30)
                "fields":  (
                    ('WAFR_SIZ', 'R4'),
                    ('DIE_HT', 'R4'),
                    ('DIE_WID', 'R4'),
                    ('WF_UNITS', 'U1'),
                    ('WF_FLAT', 'C1'),
                    ('CENTER_X', 'I2'),
                    ('CENTER_Y', 'I2'),
                    ('POS_X', 'C1'),
                    ('POS_Y', 'C1')
                )
            }, 
            
            5010: {
                "name": "Pir",  # (5, 10)
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1')
                )
                
            },
            
            5020: {
                "name": "Prr",  # (5, 20)
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('PART_FLG', 'B1'),
                    ('NUM_TEST', 'U2'),
                    ('HARD_BIN', 'U2'),
                    ('SOFT_BIN', 'U2'),
                    ('X_COORD', 'I2'),
                    ('Y_COORD', 'I2'),
                    ('TEST_T', 'U4'),
                    ('PART_ID', 'Cn'),
                    ('PART_TXT', 'Cn'),
                    ('PART_FIX', 'Bn')
                )
            },
            
            10030: {
                "name": "Tsr",  # (10, 30)
                "fields": (
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('TEST_TYP', 'C1'),
                    ('TEST_NUM', 'U4'),
                    ('EXEC_CNT', 'U4'),
                    ('FAIL_CNT', 'U4'),
                    ('ALRM_CNT', 'U4'),
                    ('TEST_NAM', 'Cn'),
                    ('SEQ_NAME', 'Cn'),
                    ('TEST_LBL', 'Cn'),
                    ('OPT_FLAG', 'B1'),
                    ('TEST_TIM', 'R4'),
                    ('TEST_MIN', 'R4'),
                    ('TEST_MAX', 'R4'),
                    ('TST_SUMS', 'R4'),
                    ('TST_SQRS', 'R4')
                )
            },
            15010: {
                "name": "Ptr",  # (15, 10)
                "fields": (
                    ('TEST_NUM', 'U4'),
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('TEST_FLG', 'B1'),
                    ('PARM_FLG', 'B1'),
                    ('RESULT', 'R4'),
                    ('TEST_TXT', 'Cn'),
                    ('ALARM_ID', 'Cn'),
                    ('OPT_FLAG', 'B1'),
                    ('RES_SCAL', 'I1'),
                    ('LLM_SCAL', 'I1'),
                    ('HLM_SCAL', 'I1'),
                    ('LO_LIMIT', 'R4'),
                    ('HI_LIMIT', 'R4'),
                    ('UNITS', 'Cn'),
                    ('C_RESFMT', 'Cn'),
                    ('C_LLMFMT', 'Cn'),
                    ('C_HLMFMT', 'Cn'),
                    ('LO_SPEC', 'R4'),
                    ('HI_SPEC', 'R4')
                )
            },
            
            15015: {
                "name": "Mpr",  # (15, 15)
                "fields": (
                    ('TEST_NUM', 'U4'),
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('TEST_FLG', 'B1'),
                    ('PARM_FLG', 'B1'),
                    ('RTN_ICNT', 'U2'),
                    ('RSLT_CNT', 'U2'),
                    ('RTN_STAT', 'K5N1'),
                    ('RTN_RSLT', 'K6R4'),
                    ('TEST_TXT', 'Cn'),
                    ('ALARM_ID', 'Cn'),
                    ('OPT_FLAG', 'B1'),
                    ('RES_SCAL', 'I1'),
                    ('LLM_SCAL', 'I1'),
                    ('HLM_SCAL', 'I1'),
                    ('LO_LIMIT', 'R4'),
                    ('HI_LIMIT', 'R4'),
                    ('START_IN', 'R4'),
                    ('INCR_IN', 'R4'),
                    ('RTN_INDX', 'K5U2'),
                    ('UNITS', 'Cn'),
                    ('UNITS_IN', 'Cn'),
                    ('C_RESFMT', 'Cn'),
                    ('C_LLMFMT', 'Cn'),
                    ('C_HLMFMT', 'Cn'),
                    ('LO_SPEC', 'R4'),
                    ('HI_SPEC', 'R4')
                )
            },
            
            15020: {
                "name": "Ftr",  # (15, 20)
                "fields": (
                    ('TEST_NUM', 'U4'),
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('TEST_FLG', 'B1'),
                    ('OPT_FLAG', 'B1'),
                    ('CYCL_CNT', 'U4'),
                    ('REL_VADR', 'U4'),
                    ('REPT_CNT', 'U4'),
                    ('NUM_FAIL', 'U4'),
                    ('XFAIL_AD', 'I4'),
                    ('YFAIL_AD', 'I4'),
                    ('VECT_OFF', 'I2'),
                    ('RTN_ICNT', 'U2'),
                    ('PGM_ICNT', 'U2'),
                    ('RTN_INDX', 'K12U2'),
                    ('RTN_STAT', 'K12N1'),
                    ('PGM_INDX', 'K13U2'),
                    ('PGM_STAT', 'K13N1'),
                    ('FAIL_PIN', 'Dn'),
                    ('VECT_NAM', 'Cn'),
                    ('TIME_SET', 'Cn'),
                    ('OP_CODE', 'Cn'),
                    ('TEST_TXT', 'Cn'),
                    ('ALARM_ID', 'Cn'),
                    ('PROG_TXT', 'Cn'),
                    ('RSLT_TXT', 'Cn'),
                    ('PATG_NUM', 'U1'),
                    ('SPIN_MAP', 'Dn')
                )
            },
            
            15030: {
                "name": "Str",  # (15, 30)
                "fields": (
                    ('CONT_FLG', 'B1'),
                    ('TEST_NUM', 'U4'),
                    ('HEAD_NUM', 'U1'),
                    ('SITE_NUM', 'U1'),
                    ('PSR_REF', 'U2'),
                    ('TEST_FLG', 'B1'),
                    ('LOG_TYP', 'Cn'),
                    ('TEST_TXT', 'Cn'),
                    ('ALARM_ID', 'Cn'),
                    ('PROG_TXT', 'Cn'),
                    ('RSLT_TXT', 'Cn'),
                    ('Z_VAL', 'U1'),
                    ('FMU_FLG', 'B1'),
                    ('MASK_MAP', 'Dn'),
                    ('FAL_MAP', 'Dn'),
                    ('CYC_CNT', 'U8'),
                    ('TOTF_CNT', 'U4'),
                    ('TOTL_CNT', 'U4'),
                    ('CYC_BASE', 'U8'),
                    ('BIT_BASE', 'U4'),
                    ('COND_CNT', 'U2'),
                    ('LIM_CNT', 'U2'),
                    ('CYC_SIZE', 'U1'),
                    ('PMR_SIZE', 'U1'),
                    ('CHN_SIZE', 'U1'),
                    ('PAT_SIZE', 'U1'),
                    ('BIT_SIZE', 'U1'),
                    ('U1_SIZE', 'U1'),
                    ('U2_SIZE', 'U1'),
                    ('U3_SIZE', 'U1'),
                    ('UTX_SIZE', 'U1'),
                    ('CAP_BGN', 'U2'),
                    ('LIM_INDX', 'K21U2'),
                    ('LIM_SPEC', 'K21U4'),
                    ('COND_LST', 'K20Cn'),
                    ('CYCO_CNT', 'U2'),
                    ('CYC_OFST', 'K35U8'),
                    ('PMR_CNT', 'U2'),
                    ('PMR_INDX', 'K37U2'),
                    ('CHN_CNT', 'U2'),
                    ('CHN_NUM', 'K39U1'),
                    ('EXP_CNT', 'U2'),
                    ('EXP_DATA', 'K41U1'),
                    ('CAP_CNT', 'U2'),
                    ('CAP_DATA', 'K43U1'),
                    ('NEW_CNT', 'U2'),
                    ('NEW_DATA', 'K45U1'),
                    ('PAT_CNT', 'U2'),
                    ('PAT_NUM', 'K47U1'),
                    ('BPOS_CNT', 'U2'),
                    ('BIT_POS', 'K49U1'),
                    ('USR1_CNT', 'U2'),
                    ('USR1', 'K51U1'),
                    ('USR2_CNT', 'U2'),
                    ('USR2', 'K53U1'),
                    ('USR3_CNT', 'U2'),
                    ('USR3', 'K55U1'),
                    ('TXT_CNT', 'U2'),
                    ('USER_TXT', 'K57U1')
                )
            },
       

            20010: {
                "name": "Bps",  # (20, 10)
                "fields": (
                    ('SEQ_NAME', 'Cn'),
                )
            },
            
            20020: {
                "name": "Eps",  # (20, 20)
                "fields": ()
                
            }, 
            
            50010: {
                "name": "Gdr",  # (50,10)
                "fields": (
                    ('GEN_DATA', 'Vn'),
                )
            },
            
            50030: {
                "name": "Dtr",  # (50, 30)
                "fields": (
                    ('TEXT_DAT', 'Cn'),
                )
            },
        }
