import re
import sqlite3
from stdf_utils import StdfRecord


class WlanTestList:
    def __init__(self, stdf_path: str):
        self.handlers = {
            "Ptr": self.ptr_handler
        }

        # sqlite connection
        db_path = re.sub(r"\.std(f)?(\.gz)?$", ".db", stdf_path)
        con = sqlite3.connect(db_path)
        self.cursor = con.cursor()
        self.create_table()

        # read
        for rec_type, rec in StdfRecord(stdf_path, set(self.handlers.keys())):
            self.handlers[rec_type](rec)

        con.commit()

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS wlan_tests(
            test_text     CHAR(256) PRIMARY KEY,
            measure_type  CHAR(16),
            direction     CHAR(4),
            protocol      CHAR(8),
            mcs           CHAR(8),
            freq          INTEGER,
            bw            CHAR(8),
            power         CHAR(64),
            result_type   CHAR(8),
            path          CHAR(2)
        );
        """)

    def ptr_handler(self, row: dict):
        test_text = row["TEST_TXT"].decode()

        RE_GAIN = re.compile(r"^a_gainX(?P<direction>(tx|rx))_(?P<protocol>11(b|n|ac|ax))"
                             r"(?P<mcs>[a-zA-Z0-9]+)_(?P<freq>\d{4})[_xX]+(?P<mask_power>m?\d+)?"
                             r"(6dbgstep)?(?P<bw>bw\d+)_"
                             r"(?P<power>([m0-9]+|step\d{2}|mask))_"
                             r"(?P<result_type>(PKT_CNT|[a-zA-Z0-9]+))_?(?P<path>\w+)?")

        m = re.search(RE_GAIN, test_text)

        if m is not None:
            measure_type = "gain"
            power = m.group("power")
            bw = m.group("bw")
            result_type = m.group("result_type")
            path = m.group("path")

            if "mask" in test_text:
                result_type = "Mask"
                path = m.group("result_type")
                power = m.group("mask_power").replace("m", "-") if m.group("mask_power") is not None else None

            elif "step" in power:  # "a_gainXtx_11nmcs0_2437_x_x__6dbgstepbw20_step01_A"
                power = power[4]
                result_type = "Gain"
                path = m.group("result_type")

            else:
                power = power.replace("m", "-")

        else:
            RE_EVM = re.compile(r"^a_evmX(?P<direction>(tx|rx))_(?P<protocol>11(b|n|ac|ax))"
                                r"(?P<mcs>[a-zA-Z0-9]+)_(?P<freq>\d{4})[_xX]+(?P<power>([m0-9]+))(?P<bw>bw\d+)_"
                                r"(rx_|a_|TXA_)?(?P<result_type>(Pout|EVM_Rms|rssi|pkt_cnt))_?(?P<path>\w+)?")
            m = RE_EVM.search(test_text)
            if m is None:
                if re.search(r"^a_(evm|gain)", test_text):
                    print(test_text)
                return

            measure_type = "evm"
            power = m.group("power").replace("m", "-")
            bw = m.group("bw")
            result_type = m.group("result_type")
            path = m.group("path")

        sql = f"""
        INSERT OR REPLACE INTO wlan_tests(test_text, measure_type, direction, protocol, mcs, freq, bw, power, result_type, path) 
        VALUES ("{test_text}", "{measure_type}", "{m.group('direction')}", "{m.group('protocol')}", "{m.group('mcs')}",
        {m.group('freq')}, "{bw}", "{power}", "{result_type}", "{path}"
        );
        """
        self.cursor.execute(sql)


if __name__ == '__main__':
    WlanTestList(
        # r"C:\log\w446_bb_a1_rf_char_evm_6g_high_stddev\20241106175704_atbk_fr1_AW693S_A1_vmax_FF_onSQU_r1393.stdf")
        r"C:\log\w448_nh_a1_ttr_nb\ref\qfn_r2gate_fresh_r2192.stdf")
