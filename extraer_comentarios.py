import pandas as pd
from apify_client import ApifyClient
import time
import re
import logging
import html
import unicodedata
import os

# Configurar logging más limpio
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# --- PARÁMETROS DE CONFIGURACIÓN ---
APIFY_TOKEN = os.environ.get("APIFY_TOKEN")
SOLO_PRIMER_POST = False

# LISTA DE URLs A PROCESAR
URLS_A_PROCESAR = [
     # TIKTOK - Links adicionales
    "https://www.tiktok.com/@alpinacol/video/7545906205563112722?_r=1&_t=ZS-8zodFWYH3fr",
    "https://www.tiktok.com/@alpinacol/video/7545906164911934736?_r=1&_t=ZS-8zodC7ZCI5a",
    "https://www.tiktok.com/@alpinacol/video/7545906204736900359?_r=1&_t=ZS-8zod8bavifI",
    "https://www.tiktok.com/@alpinacol/video/7545906203931757831?_r=1&_t=ZS-8zod3GQAOL2",
    "https://www.tiktok.com/@alpinacol/video/7545783917819858183?_r=1&_t=ZS-8zoczvlumrX",
    # INSTAGRAM - Parte 1 (1-50)
    "https://www.instagram.com/p/DOL-s8yAEpK/",
    "https://www.instagram.com/p/DOL-vK2AEau/",
    "https://www.instagram.com/p/DN8-4jkgPZ9/",
    "https://www.instagram.com/p/DN8-jkkgLpv/",
    "https://www.instagram.com/p/DOL-utugPsM/",
    "https://www.instagram.com/p/DOL-s98gFfw/",
    "https://www.instagram.com/p/DOL_ShzgF8e/",
    "https://www.instagram.com/p/DOL-m7CgKdV/",
    "https://www.instagram.com/p/DOL-mwPgJuw/",
    "https://www.instagram.com/p/DOL-oBdgA01/",
    "https://www.instagram.com/p/DOL-n6OAOXo/",
    "https://www.instagram.com/p/DOL-m3ngF3N/",
    "https://www.instagram.com/p/DOL-ms9APs0/",
    "https://www.instagram.com/p/DLYGplPAbdQ/",
    "https://www.instagram.com/p/DLYGqgsAmz2/",
    "https://www.instagram.com/p/DLYGqJhADU5/",
    "https://www.instagram.com/p/DLYGpQSAlPy/",
    "https://www.instagram.com/p/DLYGpougtjf/",
    "https://www.instagram.com/p/DLVcGUxgjko/",
    "https://www.instagram.com/p/DLVcG9ug7RA/",
    "https://www.instagram.com/p/DLYGprog1u7/",
    "https://www.instagram.com/p/DLYGp2zgy-y/",
    "https://www.instagram.com/p/DLVcEn6geo-/",
    "https://www.instagram.com/p/DLVcFVdg66e/",
    "https://www.instagram.com/p/DLYKadfAPKl/",
    "https://www.instagram.com/p/DLYKaKSAdfF/",
    "https://www.instagram.com/p/DLYKLJaAIcm/",
    "https://www.instagram.com/p/DLVcEzLgyS7/",
    "https://www.instagram.com/p/DLVcH1_g1q0/",
    "https://www.instagram.com/p/DLYKLkjg7-7/",
    "https://www.instagram.com/p/DML3IvxAwPK/",
    "https://www.instagram.com/p/DML3IGbguuF/",
    "https://www.instagram.com/p/DML3Li9A8wJ/",
    "https://www.instagram.com/p/DML3K8QA68h/",
    "https://www.instagram.com/p/DL4QNhSAIYq/",
    "https://www.instagram.com/p/DL4QNshAo2L/",
    "https://www.instagram.com/p/DL4PtoJA0Hl/",
    "https://www.instagram.com/p/DL4Pslvgttp/",
    "https://www.instagram.com/p/DL4O5DdgBFD/",
    "https://www.instagram.com/p/DL4O4MJAyQ_/",
    "https://www.instagram.com/p/DMwePMsAPeP/",
    "https://www.instagram.com/p/DMweU8MAo37/",
    "https://www.instagram.com/p/DMweWZggJCP/",
    "https://www.instagram.com/p/DMweVHpg21N/",
    "https://www.instagram.com/p/DMweWnaAbMc/",
    "https://www.instagram.com/p/DMweVoCAvDD/",
    "https://www.instagram.com/p/DMweVkfgaVJ/",
    "https://www.instagram.com/p/DMweY-CAdMw/",
    "https://www.instagram.com/p/DMweVTbgRjV/",
    "https://www.instagram.com/p/DMweVetAd2L/",
    # FACEBOOK - Parte 1 (51-100)
    "https://www.facebook.com/?feed_demo_ad=120233418145910432&h=AQCP9RxlmBjXz0LtaJAh",
    "https://www.facebook.com/?feed_demo_ad=120233417504070432&h=AQAQCH3Rl_lce0gF0_4",
    "https://www.facebook.com/?feed_demo_ad=120233417168840432&h=AQA4sGqS4BetjNxbaVU",
    "https://www.facebook.com/?feed_demo_ad=120233417074240432&h=AQB06AurVyvI00lu8K4",
    "https://www.facebook.com/?feed_demo_ad=120233054832200432&h=AQDDns3pft0Vh4Z-2iA",
    "https://www.facebook.com/?feed_demo_ad=120233063358500432&h=AQB2gbbCcn_X0UX-hOI",
    "https://www.facebook.com/?feed_demo_ad=120233055819280432&h=AQBgqCgBPhgHaSvBmjQ",
    "https://www.facebook.com/?feed_demo_ad=120233418582750432&h=AQA3C-BhfkEDKjRWGuc",
    "https://www.facebook.com/?feed_demo_ad=120233555370800432&h=AQBWtXEcnrVRvvpNVEk",
    "https://www.facebook.com/?feed_demo_ad=120233418238300432&h=AQCshb4xg4w3aZQNhso",
    "https://www.facebook.com/?feed_demo_ad=120233554870910432&h=AQBbgdd3MGvw9q6WCwY",
    "https://www.facebook.com/?feed_demo_ad=120233417504070432&h=AQAQCH3Rl_lce0gF7k0",
    "https://www.facebook.com/?feed_demo_ad=120229951075780528&h=AQDbOSv64-iYeSAEiVw",
    "https://www.facebook.com/?feed_demo_ad=120229951045890528&h=AQCdiPQDrtwWYQzSEac",
    "https://www.facebook.com/?feed_demo_ad=120229865720060528&h=AQCVNSdcEXmYfiZ2i8s",
    "https://www.facebook.com/?feed_demo_ad=120229865720050528&h=AQCPveHy_wGlatdLLYs",
    "https://www.facebook.com/?feed_demo_ad=120229951366600528&h=AQCrHs8DzSaG6jZK1Hc",
    "https://www.facebook.com/?feed_demo_ad=120229866278060528&h=AQCT7R4GoekvtyxQkKc",
    "https://www.facebook.com/?feed_demo_ad=120229866278040528&h=AQC2LerMXcpyYvbDibc",
    "https://www.facebook.com/?feed_demo_ad=120229951331730528&h=AQAvC7NYc7eqU6Dyhhk",
    "https://www.facebook.com/?feed_demo_ad=120229951316800528&h=AQDkEBNVP6rnG-DV-CE",
    "https://www.facebook.com/?feed_demo_ad=120229865596040528&h=AQDNLQOiuPbr2TiBQN8",
    "https://www.facebook.com/?feed_demo_ad=120229865343090528&h=AQAn-x1c2oOQ-lg8RRU",
    "https://www.facebook.com/?feed_demo_ad=120229953175120528&h=AQB-qUzudBzSGazwqHY",
    "https://www.facebook.com/?feed_demo_ad=120229953175110528&h=AQDnep_ijCIj8FB_R8c",
    "https://www.facebook.com/?feed_demo_ad=120229866799520528&h=AQC3NJRG7viPLJw2Y88",
    "https://www.facebook.com/?feed_demo_ad=120229866799440528&h=AQBaRlBk_g1c4OqJgcA",
    "https://www.facebook.com/?feed_demo_ad=120229952909570528&h=AQBzoZlY-ggEW7O0lVk",
    "https://www.facebook.com/?feed_demo_ad=120229952909560528&h=AQCm2HPsnSjjSNOG3mI",
    "https://www.facebook.com/?feed_demo_ad=120229952822970528&h=AQDa-LCLw-pnsWjxmVc",
    "https://www.facebook.com/?feed_demo_ad=120229866799510528&h=AQCcYGBOMlUkzhVRebM",
    "https://www.facebook.com/?feed_demo_ad=120229866799480528&h=AQCGyJvZ2XItfQePWaI",
    "https://www.facebook.com/?feed_demo_ad=120229951659330528&h=AQB0Sj5szF-x73HcVnY",
    "https://www.facebook.com/?feed_demo_ad=120229866799500528&h=AQCJCPLff1-FljSmDDQ",
    "https://www.facebook.com/?feed_demo_ad=120229866799470528&h=AQBCErmlALvWdsreXyw",
    "https://www.facebook.com/?feed_demo_ad=120231110018570528&h=AQAApjd1DuFCNQBRjbk",
    "https://www.facebook.com/?feed_demo_ad=120231110018530528&h=AQCpfyXVsahkuXex6t4",
    "https://www.facebook.com/?feed_demo_ad=120231110077490528&h=AQCvl_23wQlX19Dfh80",
    "https://www.facebook.com/?feed_demo_ad=120231110077450528&h=AQCwK9_PKyIK0dn-0_k",
    "https://www.facebook.com/?feed_demo_ad=120230702158950528&h=AQDwrFy9MaMIFCz-3CU",
    "https://www.facebook.com/?feed_demo_ad=120230702158960528&h=AQA3AIPXUyDdCU4scjA",
    "https://www.facebook.com/?feed_demo_ad=120230976119770528&h=AQDPqvwBI76suz-KsU4",
    "https://www.facebook.com/?feed_demo_ad=120230976119780528&h=AQDQxGtlp7EFwr8luMQ",
    "https://www.facebook.com/?feed_demo_ad=120231001991260528&h=AQBbMOPVw1dAC6R6NKc",
    "https://www.facebook.com/?feed_demo_ad=120231001991290528&h=AQAahM5tpIO8vMgruOU",
    "https://www.facebook.com/?feed_demo_ad=120230701950360528&h=AQA1VTnLbFDhVXk3_N8",
    "https://www.facebook.com/?feed_demo_ad=120230701950370528&h=AQAXnTyWFOupEgKN0GY",
    "https://www.facebook.com/?feed_demo_ad=120231001991280528&h=AQANwaIUD7T0AfzVQ58",
    "https://www.facebook.com/?feed_demo_ad=120231001991270528&h=AQBk2nsBgbrXm5ZalKI",
    "https://www.facebook.com/?feed_demo_ad=120230701719800528&h=AQD-HB1JfE4fL-6ZYwg",
    "https://www.facebook.com/?feed_demo_ad=120230701719790528&h=AQBopnumRdAuGco2UJo",
     # Continuación URLS_A_PROCESAR - Links 101-200
    # INSTAGRAM - Parte 2 (continuación)
    "https://www.instagram.com/p/DMweWN-AdyD/",
    "https://www.instagram.com/p/DMweV8iAFAZ/",
    "https://www.instagram.com/p/DMweWOlgwss/",
    "https://www.instagram.com/p/DMweWSPAYEg/",
    "https://www.instagram.com/p/DMwePClA_lw/",
    "https://www.instagram.com/p/DMweVJ4g2zO/",
    "https://www.instagram.com/p/DMweWIPguGI/",
    "https://www.instagram.com/p/DMgNJbfAT7f/",
    "https://www.instagram.com/p/DMgNN0AgccI/",
    "https://www.instagram.com/p/DMgNKZpALzq/",
    "https://www.instagram.com/p/DMgNKQFAv4A/",
    "https://www.instagram.com/p/DMgNJjAgscf/",
    "https://www.instagram.com/p/DMgNKmIAcNt/",
    "https://www.instagram.com/p/DMgNJa3glbO/",
    "https://www.instagram.com/p/DMweWsaAtMI/",
    "https://www.instagram.com/p/DMsbNiPgR1L/",
    "https://www.instagram.com/p/DNNIFItgyq7/",
    "https://www.instagram.com/p/DM-vgmdgURu/",
    "https://www.instagram.com/p/DM-vfZsg4fC/",
    "https://www.instagram.com/p/DM-vgkSAONh/",
    "https://www.instagram.com/p/DM-vfMlg05H/",
    "https://www.instagram.com/p/DOJ1IfZgMkK/",
    "https://www.instagram.com/p/DOKy5xrgAWJ/",
    "https://www.instagram.com/p/DOKy5uTgKUO/",
    "https://www.instagram.com/p/DOKy5PLADHq/",
    "https://www.instagram.com/p/DOKy5cEAAcQ/",
    "https://www.instagram.com/p/DOKy5ejgFaV/",
    "https://www.instagram.com/p/DOJ1Iv0ABvo/",
    "https://www.instagram.com/p/DOKy5sBAAWP/",
    "https://www.instagram.com/p/DOJ1IvXAI4e/",
    "https://www.instagram.com/p/DOJ1I3bACvH/",
    "https://www.instagram.com/p/DOKy5fiALE9/",
    "https://www.instagram.com/p/DOKy5rdAPK6/",
    "https://www.instagram.com/p/DOJ1I-ngOqh/",
    "https://www.instagram.com/p/DOKy5iDAIdy/",
    "https://www.instagram.com/p/DOJk2bpgKSF/",
    "https://www.instagram.com/p/DOKy589gPp1/",
    "https://www.instagram.com/p/DOKy5iQANz7/",
    "https://www.instagram.com/p/DOKy50FgF6j/",
    "https://www.instagram.com/p/DOJ1I-8AJtl/",
    "https://www.instagram.com/p/DOKy5q3AKsY/",
    "https://www.instagram.com/p/DOJk2MUgE8f/",
    "https://www.instagram.com/p/DOKy5WUAEwC/",
    "https://www.instagram.com/p/DOKy5uwgBs0/",
    "https://www.instagram.com/p/DOKy5aQgLAr/",
    "https://www.instagram.com/p/DOJ1I0TgLBE/",
    "https://www.instagram.com/p/DOJk1DsgF9F/",
    "https://www.instagram.com/p/DOJ1IKDAN8A/",
    "https://www.instagram.com/p/DOKy5jMgKDI/",
    "https://www.instagram.com/p/DOKy5gcAHFr/",
    # FACEBOOK - Parte 2 (continuación)
    "https://www.facebook.com/?feed_demo_ad=120232172592130528&h=AQAaIvEfklka7IgeMus",
    "https://www.facebook.com/?feed_demo_ad=120232172592120528&h=AQBrg8RGcsqe70ibv5c",
    "https://www.facebook.com/?feed_demo_ad=120232172592110528&h=AQAebGaMjrduSzyMIBo",
    "https://www.facebook.com/?feed_demo_ad=120232172592100528&h=AQARe0xfqJTOg3ONTvI",
    "https://www.facebook.com/?feed_demo_ad=120232172592090528&h=AQBzk6D5BekETQxOh1Q",
    "https://www.facebook.com/?feed_demo_ad=120232172592080528&h=AQDDAPiPV3pRNRDQxUU",
    "https://www.facebook.com/?feed_demo_ad=120232172592070528&h=AQA5axHtWYPBCEG_GW8",
    "https://www.facebook.com/?feed_demo_ad=120232172592060528&h=AQAoVFCnvyWK3ScVjPc",
    "https://www.facebook.com/?feed_demo_ad=120232172592050528&h=AQAwwv8kMQbvL35ZiYc",
    "https://www.facebook.com/?feed_demo_ad=120232172592040528&h=AQAmvbN7T4Jsf9ZAq1I",
    "https://www.facebook.com/?feed_demo_ad=120232172592030528&h=AQBejetq_kkfnOb8psA",
    "https://www.facebook.com/?feed_demo_ad=120232172592020528&h=AQA0HzoKRhW-R2SNwNY",
    "https://www.facebook.com/?feed_demo_ad=120232172592010528&h=AQAtRbggGWB0cPkw-rQ",
    "https://www.facebook.com/?feed_demo_ad=120232172592000528&h=AQBNclVqkajjAomn3xA",
    "https://www.facebook.com/?feed_demo_ad=120232172591990528&h=AQBYVy_qBqE1gCo9t-A",
    "https://www.facebook.com/?feed_demo_ad=120232172591980528&h=AQCwvaP8oJQoPgZPOmo",
    "https://www.facebook.com/?feed_demo_ad=120232172304290528&h=AQC9uzDj9Tw50dkuaGA",
    "https://www.facebook.com/?feed_demo_ad=120231631264620528&h=AQCHnahyw9vvJbErOOY",
    "https://www.facebook.com/?feed_demo_ad=120231631264600528&h=AQASnA_cqK1lZ3vg9Qs",
    "https://www.facebook.com/?feed_demo_ad=120231631264570528&h=AQDW2mDjmUK8wzCBh-k",
    "https://www.facebook.com/?feed_demo_ad=120231631264480528&h=AQDfqMWBe0Ap9wEbnvU",
    "https://www.facebook.com/?feed_demo_ad=120231631264470528&h=AQDDnTlKbc7GZ9pKbOQ",
    "https://www.facebook.com/?feed_demo_ad=120231631264460528&h=AQDLCFMIG0Jog8dFPuo",
    "https://www.facebook.com/?feed_demo_ad=120231631264450528&h=AQBhIFsNRi-oNOE3jH8",
    "https://www.facebook.com/?feed_demo_ad=120232172592140528&h=AQC4Bf5dTfp7Oqn0c30",
    "https://www.facebook.com/?feed_demo_ad=120232176271180528&h=AQAa6XgCQVO7iaDn7S4",
    "https://www.facebook.com/?feed_demo_ad=120232176271170528&h=AQCRkWG7fbjmiVprfF8",
    "https://www.facebook.com/?feed_demo_ad=120232176271160528&h=AQCZxoJ1gjFiErT_30o",
    "https://www.facebook.com/?feed_demo_ad=120232176271150528&h=AQDFherdKOiGvp-tdAU",
    "https://www.facebook.com/?feed_demo_ad=120232176271140528&h=AQBlOpA_1ffnCjSzghA",
    "https://www.facebook.com/?feed_demo_ad=120232176271130528&h=AQD0WlsFOJFPhHdqKoA",
    "https://www.facebook.com/?feed_demo_ad=120232176271120528&h=AQClH2rx6Vy2DuBCkko",
    "https://www.facebook.com/?feed_demo_ad=120232176271110528&h=AQCJjYC-W6IH47NUldc",
    "https://www.facebook.com/?feed_demo_ad=120232176271100528&h=AQArm_UCDhlOSS6bwg8",
    "https://www.facebook.com/?feed_demo_ad=120232176271090528&h=AQCo36KQ-yCkJ5WO744",
    "https://www.facebook.com/?feed_demo_ad=120232176271080528&h=AQD5uGNzXaY_zcM2X4A",
    "https://www.facebook.com/?feed_demo_ad=120232176271070528&h=AQCGMBDfk2vKVN3_efk",
    "https://www.facebook.com/?feed_demo_ad=120232176271060528&h=AQBbTIhrTXHZsgBQeUg",
    "https://www.facebook.com/?feed_demo_ad=120232176271050528&h=AQAnbZ4je4ysj8Qg2Gs",
    "https://www.facebook.com/?feed_demo_ad=120232176271040528&h=AQAVaw3xz5UL-RN2Pdk",
    "https://www.facebook.com/?feed_demo_ad=120232176271030528&h=AQBJCuQiSokPS2cS4n0",
    "https://www.facebook.com/?feed_demo_ad=120232176271020528&h=AQBi7A2dKupPBDNZ5h4",
    "https://www.facebook.com/?feed_demo_ad=120232176271010528&h=AQDKW6Vv01_7MvwhyNc",
    "https://www.facebook.com/?feed_demo_ad=120232067268890528&h=AQCs7rY6peUqrPbjTtA",
    "https://www.facebook.com/?feed_demo_ad=120232067498230528&h=AQCuDB2OVEruPLv-ZL4",
    "https://www.facebook.com/?feed_demo_ad=120232807081760528&h=AQB-da-vPOvrLTa30aM",
    "https://www.facebook.com/?feed_demo_ad=120232550474620528&h=AQDHpATbJlAsp71x-mg",
    "https://www.facebook.com/?feed_demo_ad=120232550474590528&h=AQADw7y3iNl_qHvZ124",
    "https://www.facebook.com/?feed_demo_ad=120232550474600528&h=AQD5N59YlJO6vZuGMKA",
    "https://www.facebook.com/?feed_demo_ad=120232550474610528&h=AQC1_EKNAAg7zcd31Jk",
    "https://www.facebook.com/?feed_demo_ad=120234317977680528&h=AQBvZPv-3bvs8SJ8byc",
    # Continuación URLS_A_PROCESAR - Links 201-300
    # FACEBOOK - Parte 3 (continuación)
    "https://www.facebook.com/?feed_demo_ad=120234198744780528&h=AQAEu-V1J3rvOBnhk_A",
    "https://www.facebook.com/?feed_demo_ad=120234237928540528&h=AQArOkfYaG2w25Jo2vA",
    "https://www.facebook.com/?feed_demo_ad=120234199096330528&h=AQALGqizsR8GVZkEem4",
    "https://www.facebook.com/?feed_demo_ad=120234237275450528&h=AQCo3u5swn6A3SBi4-E",
    "https://www.facebook.com/?feed_demo_ad=120234197141210528&h=AQDc1IVqYETXIgYP1Ak",
    "https://www.facebook.com/?feed_demo_ad=120234316601330528&h=AQDGAnMh7qqDyK_yWtI",
    "https://www.facebook.com/?feed_demo_ad=120234198421170528&h=AQB06GHBgdqkAB8GJT4",
    "https://www.facebook.com/?feed_demo_ad=120234310003560528&h=AQB9GvIGBtCsB3RxO6A",
    "https://www.facebook.com/?feed_demo_ad=120234317015340528&h=AQDrOMxQfgBdwsaP7yo",
    "https://www.facebook.com/?feed_demo_ad=120234241726820528&h=AQAD2ZJjVjkB8WdeP4E",
    "https://www.facebook.com/?feed_demo_ad=120234237653380528&h=AQAwtptR2E9Epipk_5M",
    "https://www.facebook.com/?feed_demo_ad=120234318125910528&h=AQAh5kI5WMn5K0ghXM4",
    "https://www.facebook.com/?feed_demo_ad=120234238170860528&h=AQDLsgawJjxAfF0XcOY",
    "https://www.facebook.com/?feed_demo_ad=120234308557670528&h=AQDHPyIi3-EcN8vrEnE",
    "https://www.facebook.com/?feed_demo_ad=120234241334590528&h=AQDk3i1Hb_Ndw9Td9J0",
    "https://www.facebook.com/?feed_demo_ad=120234237737840528&h=AQBFfiHPf2FCqee3C0M",
    "https://www.facebook.com/?feed_demo_ad=120234239144160528&h=AQBMZm5FiF2IYdvN8wE",
    "https://www.facebook.com/?feed_demo_ad=120234309959970528&h=AQBmCbg77rT0HheZoJ8",
    "https://www.facebook.com/?feed_demo_ad=120234240744960528&h=AQDyqwsjA_tIi-xmR0w",
    "https://www.facebook.com/?feed_demo_ad=120234307839770528&h=AQBQuJ8O86vFkIcgsbo",
    "https://www.facebook.com/?feed_demo_ad=120234241676350528&h=AQC6W3KtNDnAQO5TZQA",
    "https://www.facebook.com/?feed_demo_ad=120234241803710528&h=AQAsYfbv6eFAHMAHeSo",
    "https://www.facebook.com/?feed_demo_ad=120234236557040528&h=AQBh66jw5evUpb9gt9c",
    "https://www.facebook.com/?feed_demo_ad=120234317914330528&h=AQD9C7aypz5aW-atXyM",
    "https://www.facebook.com/?feed_demo_ad=120234308967340528&h=AQAMCUJO8WV81nVE3_Q",
    "https://www.facebook.com/?feed_demo_ad=120234309744400528&h=AQB_AVy55ncEONWuaGQ",
    "https://www.facebook.com/?feed_demo_ad=120234239022710528&h=AQBMLe6aDbqPF_T1Ovo",
    "https://www.facebook.com/?feed_demo_ad=120234198935170528&h=AQBB9WlK7cqn3hjaqPw",
    "https://www.facebook.com/?feed_demo_ad=120234318260400528&h=AQAyU7lKiDfigdtRLHw",
    "https://www.facebook.com/?feed_demo_ad=120234236312470528&h=AQARTOPjewPSQmFb8wo",
    "https://www.facebook.com/?feed_demo_ad=120234238361610528&h=AQDCnf8yAtLks4Dyz5k",
    "https://www.facebook.com/?feed_demo_ad=120234309788000528&h=AQANi1kfHrRIadN6onA",
    "https://www.facebook.com/?feed_demo_ad=120234239233720528&h=AQCE0OoUgwgjExRHpZk",
    "https://www.facebook.com/?feed_demo_ad=120234308798670528&h=AQBQ31Qea_ObC9c7TVc",
    "https://www.facebook.com/?feed_demo_ad=120234317817200528&h=AQDI1uP9lj-YBkJNPsE",
    "https://www.facebook.com/?feed_demo_ad=120234239536130528&h=AQCioJA53g0RXqVBuE0",
    "https://www.facebook.com/?feed_demo_ad=120234307377020528&h=AQDvBc9yyQ2YZoogNAM",
    "https://www.facebook.com/?feed_demo_ad=120234236454060528&h=AQA6JM0XvTspvU3mYgM",
    "https://www.facebook.com/?feed_demo_ad=120234239394020528&h=AQBk3P59dpuDyRu3YMA",
    "https://www.facebook.com/?feed_demo_ad=120234318359350528&h=AQDRn6fUXhWQMHYUDhY",
    "https://www.facebook.com/?feed_demo_ad=120234237478820528&h=AQDc0-O4SWkHOv2pgSQ",
    "https://www.facebook.com/?feed_demo_ad=120234237827000528&h=AQBHxEEBHw7w1Zzxuys",
    "https://www.facebook.com/?feed_demo_ad=120234308362000528&h=AQDVrjrFfQcvyD6y3c8",
    "https://www.facebook.com/?feed_demo_ad=120233994998930528&h=AQC-7PhOtzXwl4Qe6aI",
    "https://www.facebook.com/?feed_demo_ad=120233995048030528&h=AQDCfBe-uxELRTQkBvk",
    "https://www.facebook.com/?feed_demo_ad=120233995048140528&h=AQANKHw0FT_pAZLJ_4A",
    "https://www.facebook.com/?feed_demo_ad=120233995048180528&h=AQBgvEcJ0eebSovxfjk",
    "https://www.facebook.com/?feed_demo_ad=120233995048160528&h=AQB7Vn-y_3yI6o9cuqI",
    "https://www.facebook.com/?feed_demo_ad=120233994697320528&h=AQBTDinI3Flzp_mGUvc",
    "https://www.facebook.com/?feed_demo_ad=120233991477580528&h=AQD81q9sSSl19QEPpJQ",
    # INSTAGRAM - Parte 3 (continuación)
    "https://www.instagram.com/p/DOJ1JAhgH2b/",
    "https://www.instagram.com/p/DOKy5orgCHj/",
    "https://www.instagram.com/p/DOKy5k-AOiR/",
    "https://www.instagram.com/p/DOJ1I3AAM4q/",
    "https://www.instagram.com/p/DOKy5a4AEX0/",
    "https://www.instagram.com/p/DOJk2mAACPq/",
    "https://www.instagram.com/p/DOJ1KKDANI7/",
    "https://www.instagram.com/p/DOKy51xAIei/",
    "https://www.instagram.com/p/DOJh3vLgPqb/",
    "https://www.instagram.com/p/DOKy5eYAHJn/",
    "https://www.instagram.com/p/DOKy6OrACfT/",
    "https://www.instagram.com/p/DOJ1HuCgM5g/",
    "https://www.instagram.com/p/DOKy5fJAIr4/",
    "https://www.instagram.com/p/DOKy5dDAGfg/",
    "https://www.instagram.com/p/DOJk25pgKiT/",
    "https://www.instagram.com/p/DN57pB1jERi/",
    "https://www.instagram.com/p/DN5_P5ZjLG3/",
    "https://www.instagram.com/p/DN59teFAGER/",
    "https://www.instagram.com/p/DN59tauAKm2/",
    "https://www.instagram.com/p/DN59wfOjMs2/",
    "https://www.instagram.com/p/DN57o8vjFGo/",
    "https://www.instagram.com/p/DN57pBUgFSK/",
    "https://www.instagram.com/p/DN5_STUFZM-/",
    "https://www.instagram.com/p/DN59tq0j1Ph/",
    "https://www.instagram.com/p/DN59tsyAFZ7/",
    "https://www.instagram.com/p/DN5_PjRjI2R/",
    "https://www.instagram.com/p/DN5_RL7DxSB/",
    "https://www.instagram.com/p/DN5_S8hDx4V/",
    "https://www.instagram.com/p/DN59sU1DANX/",
    "https://www.instagram.com/p/DN5_P4RjDXp/",
    "https://www.instagram.com/p/DN57rvEjAd3/",
    "https://www.instagram.com/p/DN5_Pt-gLSZ/",
    "https://www.instagram.com/p/DN5_P3ljOET/",
    "https://www.instagram.com/p/DN59wdzjdKJ/",
    "https://www.instagram.com/p/DN59xBtjADu/",
    "https://www.instagram.com/p/DN57rWvjPIN/",
    "https://www.instagram.com/p/DN59xRNDHUE/",
    "https://www.instagram.com/p/DN57ru6DMwD/",
    "https://www.instagram.com/p/DN596MUj3av/",
    "https://www.instagram.com/p/DN5_StEAAQh/",
    "https://www.instagram.com/p/DN57ojQAEEg/",
    "https://www.instagram.com/p/DN59wEujyAe/",
    "https://www.instagram.com/p/DN57rD8DEKT/",
    "https://www.instagram.com/p/DN5_RuSgNdP/",
    "https://www.instagram.com/p/DN59wgZjE3H/",
    "https://www.instagram.com/p/DN5_Qt7ALPO/",
    "https://www.instagram.com/p/DN5_QZCgPIS/",
    "https://www.instagram.com/p/DN57qGFj2R1/",
    "https://www.instagram.com/p/DN59yVXgMYH/",
    "https://www.instagram.com/p/DN57rF5EYlO/",
    "https://www.instagram.com/p/DN57qXtDMzI/",
    "https://www.instagram.com/p/DOYjdr3gMwG/",
    "https://www.instagram.com/p/DOYjdvAgIxV/",
    "https://www.instagram.com/p/DOYjda3AET_/",
    "https://www.instagram.com/p/DOYjd5igBtb/",
    "https://www.instagram.com/p/DOYjdsPgNIQ/",
    "https://www.instagram.com/p/DOYjeCiAOsO/",
    "https://www.instagram.com/p/DOYjdt_gLRQ/",
    "https://www.instagram.com/p/DOYjcncgOUZ/",
    "https://www.instagram.com/p/DOYjdLYAJr9/",
    "https://www.instagram.com/p/DOYjcsmgISX/",
    "https://www.instagram.com/p/DOYjcmUALj3/",
    "https://www.instagram.com/p/DOYjczjAM7v/",
     # Continuación URLS_A_PROCESAR - Links 301-400
    # FACEBOOK - Parte 4 (continuación)
    "https://www.facebook.com/?feed_demo_ad=120233995048000528&h=AQDec0zvTJVRA4_u5y4",
    "https://www.facebook.com/?feed_demo_ad=120233995048190528&h=AQDO4Oj_JPK8l8iHn6c",
    "https://www.facebook.com/?feed_demo_ad=120233995048170528&h=AQBJ-BsvuIafUeEyzCo",
    "https://www.facebook.com/?feed_demo_ad=120233995048020528&h=AQDqBcoLcDqWTs5Fgbo",
    "https://www.facebook.com/?feed_demo_ad=120233995047970528&h=AQBsQTvQuLyZizKY6hU",
    "https://www.facebook.com/?feed_demo_ad=120233995047960528&h=AQDC7mgGZIZVoJZe6dM",
    "https://www.facebook.com/?feed_demo_ad=120233995048110528&h=AQAYHS1nlmFyQ4tt_jg",
    "https://www.facebook.com/?feed_demo_ad=120233995047980528&h=AQDDu3OgYX8L1UUIB-s",
    "https://www.facebook.com/?feed_demo_ad=120233993986740528&h=AQDCDH4U-eiQ8kFZIWI",
    "https://www.facebook.com/?feed_demo_ad=120233995048010528&h=AQBE9ixiTV06S32J9Ws",
    "https://www.facebook.com/?feed_demo_ad=120233995048060528&h=AQB7U_7NMDrH-UvcQMk",
    "https://www.facebook.com/?feed_demo_ad=120233995048150528&h=AQABjmAFxjAoZh5EwVw",
    "https://www.facebook.com/?feed_demo_ad=120233995048090528&h=AQAX1rpT91itUflEE1M",
    "https://www.facebook.com/?feed_demo_ad=120233994823750528&h=AQCG2VveihnRp1MxRzc",
    "https://www.facebook.com/?feed_demo_ad=120233995048120528&h=AQBLBHsPU_StePdyVVA",
    "https://www.facebook.com/?feed_demo_ad=120233994823760528&h=AQAxlKtBvA-J3WqhKYE",
    "https://www.facebook.com/?feed_demo_ad=120233994928910528&h=AQDUAG6LRrtPx_cM8UI",
    "https://www.facebook.com/?feed_demo_ad=120233995048050528&h=AQAKb4_0xSrzVgUsVLk",
    "https://www.facebook.com/?feed_demo_ad=120233994928900528&h=AQBm2LIiexVe9HvkbL0",
    "https://www.facebook.com/?feed_demo_ad=120233995048080528&h=AQB6EW9fLYEykdC0yqA",
    "https://www.facebook.com/?feed_demo_ad=120233994705960528&h=AQAAhBuve_7IbbmlArY",
    "https://www.facebook.com/?feed_demo_ad=120233995048040528&h=AQBj16l8P7NuDRAqygc",
    "https://www.facebook.com/?feed_demo_ad=120233995048100528&h=AQAcR6tkLLA_11gHjYM",
    "https://www.facebook.com/?feed_demo_ad=120233995047990528&h=AQCB37gks2dqFBAThS8",
    "https://www.facebook.com/?feed_demo_ad=120233995048070528&h=AQCoaVWJRP7e3fsJAIo",
    "https://www.facebook.com/?feed_demo_ad=120233993986720528&h=AQBVDcBKhoDww_RVlr4",
    "https://www.facebook.com/?feed_demo_ad=120233995048130528&h=AQDrNPLcwGAGRGPsHas",
    "https://www.facebook.com/?feed_demo_ad=120233993986730528&h=AQDDxgkUkY_X66-DEac",
    "https://www.facebook.com/?feed_demo_ad=120233994998940528&h=AQDJs9D-atUpub4oPCg",
    "https://www.facebook.com/?feed_demo_ad=120234668517270528&h=AQCA-XaYVDsc9izy908",
    "https://www.facebook.com/?feed_demo_ad=120234668516960528&h=AQA3o7nPW8R7jkMuWAo",
    "https://www.facebook.com/?feed_demo_ad=120234668516970528&h=AQBMZUzcJv38cVPqozA",
    "https://www.facebook.com/?feed_demo_ad=120234668517060528&h=AQBHP3w_eXArpk7xjqY",
    "https://www.facebook.com/?feed_demo_ad=120234668517160528&h=AQCAL8wo0djxU1XX23Q",
    "https://www.facebook.com/?feed_demo_ad=120234668517120528&h=AQDmJUDmRujh30MdApA",
    "https://www.facebook.com/?feed_demo_ad=120234668517210528&h=AQD_qAo0gtb_oV_cNLI",
    "https://www.facebook.com/?feed_demo_ad=120234668517010528&h=AQAP5_rYXjitsGni-wE",
    "https://www.facebook.com/?feed_demo_ad=120234668516990528&h=AQCZuwpbWaMGknIzt1E",
    "https://www.facebook.com/?feed_demo_ad=120234668517030528&h=AQCRbxa_mgrWbbyLfhA",
    "https://www.facebook.com/?feed_demo_ad=120234668517180528&h=AQBtiaD3R8jcMlvGke0",
    "https://www.facebook.com/?feed_demo_ad=120234668517170528&h=AQBEXaB9MktkZKnKd6Y",
    # FACEBOOK - Posts
    "https://www.facebook.com/100064867445065/posts/pfbid02tU1gU4dpz5E4hdSJQbQfHHRWQeebhrGLvGDdRwCfEENUfiJM7LBKg2ZXL8xjh6Qdl?dco_ad_id=120233418145910432",
    "https://www.facebook.com/100064867445065/posts/pfbid09ESTvbRgHFXPvPnzu5Wuxc17mLHGzbvh3PXviVmWXTC63LtA7fwKWhoAmnoPCGLKl?dco_ad_id=120233417504070432",
    "https://www.facebook.com/100064867445065/posts/pfbid0vhyQ3E2hLKNx7xAhucBeXJMXJQRevYeXPBqbjv8aLadNxYp5XSnYdasEHXTmTf4gl?dco_ad_id=120233417168840432",
    "https://www.facebook.com/100064867445065/posts/pfbid02M6noi2G6CkPLtfRGQEzZw48B7i2pGmk2KrDTUixNn4inUUrxG47VFVVtsgjsVpGXl?dco_ad_id=120233417074240432",
    "https://www.facebook.com/100064867445065/posts/pfbid04uas4JSTf7CfNJ7vygqn7LEq2kaRS43o2TgqHWuttDDG6A6DnrCLjtP2MFScAZwbl?dco_ad_id=120233054832200432",
    "https://www.facebook.com/100064867445065/posts/pfbid0neLQsQFk4CkV8yBRRzcXEdBtA91Wt3C6RrHFz4QaUZevWDh1SzqUPCcWHiN31f3xl?dco_ad_id=120233063358500432",
    "https://www.facebook.com/100064867445065/posts/pfbid0KRNkHEJBgq8mga9zMUZCcxGF44ZFTkDCR79aecj6UMVc1guCGFs1nxvxVMecBu8hl?dco_ad_id=120233055819280432",
    "https://www.facebook.com/100064867445065/posts/pfbid02DMBD3zQxW4n6oSVNcbBR4CQZvuFtDVvrQTo7qBkFDnUXcCNzxzDK79rAk68C3kv5l?dco_ad_id=120233417504070432",
    "https://www.facebook.com/100064867445065/posts/pfbid0VJAHuEFyAuzTRm5fxqGxTcH5LtfbrEExF87mKchBduuFVu7CJJJDBJHwmSajwLYyl?dco_ad_id=120232172592120528",
    "https://www.facebook.com/100064867445065/posts/pfbid0QoobJ4yFwFs1g7jDW5aFqJpxdjWCS3nmL7awRyu2TvXHQwHF8BFNWYEQCyLCN42rl?dco_ad_id=120232172592110528",
    "https://www.facebook.com/100064867445065/posts/pfbid0rfayMJkDEYuewJnTjknbZtoRc5ML8Lpu8u4cGZ6U4kW9eVJnKGQ676dDodWDJ1Bgl?dco_ad_id=120232172592100528",
    "https://www.facebook.com/100064867445065/posts/pfbid028rFH8142jR7YpKwTRS7zNmeQyXAJcXFSU4gtsbfetLEmTVViX3WnHozKNzEr7mfRl?dco_ad_id=120232172592090528",
    "https://www.facebook.com/100064867445065/posts/pfbid0vPpfJb7yQdDAdGmfWLQW8rK9nEfws7a1UqRPetnRhUDVB2GAFvyS8qyj2qMd4i1wl?dco_ad_id=120232172592080528",
    "https://www.facebook.com/100064867445065/posts/pfbid02SYAed6fygG98yjMiDNPuMhbdvjei8y4gsnWQQwNbduym5jhD9ht1umRXMAXLybSEl?dco_ad_id=120232172592070528",
    "https://www.facebook.com/100064867445065/posts/pfbid02XiAfBpvsuWaDtsaQSGtdWLzs8ydC1HEmiiSTYoE8vfwQnT1oB9pw3dNkMLoAECu6l?dco_ad_id=120232172592060528",
    "https://www.facebook.com/100064867445065/posts/pfbid0Tk4ZenxoRcho7NXqkLcMpJXxZcFFyxDhhAjCYzMkYAWM67zNKoBVpJm673QaiX7nl?dco_ad_id=120232172592050528",
    "https://www.facebook.com/100064867445065/posts/pfbid02mWEtBLUhJYvGc5QarCVp1wFe8CaMXw7QGKdzpFvc4FMrxtjB1tMe2jWf2w9YQkJAl?dco_ad_id=120232172592040528",
    "https://www.facebook.com/100064867445065/posts/pfbid0gXAMN6bRLjQn886Qz9eKbC4Vq1qh7vME1hSvedVF7TvYY4EG67WDpuZ1KwCKCRXKl?dco_ad_id=120232172592030528",
    "https://www.facebook.com/100064867445065/posts/pfbid0ro82mHpfHyVekbtHu9XCXTDLDzrEpnZm3wQhaYFW1tfvBizfnRFi1R4Vyj6caNw7l?dco_ad_id=120232172592020528",
    "https://www.facebook.com/100064867445065/posts/pfbid034H5qdwJB4QWzAy1z1ErqdZ74LPrjd7AMzM7HHgS7GhYwuYD2P4jrHaUxcXQcoMQKl?dco_ad_id=120232172592010528",
    "https://www.facebook.com/100064867445065/posts/pfbid02c5ke4RRZaiehrX3ofU1ArU6gd4frEFwf5Q2hbtRD1b12c7HmUbgA6xPvpBBmz57Kl?dco_ad_id=120232172592000528",
    "https://www.facebook.com/100064867445065/posts/pfbid0qv1dcSc8pKVF6G8H3mT6AKFoZvGUCvea4Jaw8f3NsvL4XKfpS8nnDgqAN5Ti7HMQl?dco_ad_id=120232172591980528",
    "https://www.facebook.com/100064867445065/posts/pfbid0bWt7Je9ZaByYKnXyS9p5y4eHayAZANV2cNxMwAUH8YqeFxJ4zgT28urKKpe1iXMNl?dco_ad_id=120232172304290528",
    "https://www.facebook.com/100064867445065/posts/pfbid0VQAeX98UPdrbhDBUhofLhqxmAM3oasJL2etqeAVzVqju1dmhKMMe2JAcANEJBPPal?dco_ad_id=120231631264620528",
    "https://www.facebook.com/100064867445065/posts/pfbid028k2FjgfyeznBAMn1ahRNXTVcGexkJFBqrGTWnwy96SHZ5inLGekYq8MnZawiuFf6l?dco_ad_id=120231631264600528",
    "https://www.facebook.com/100064867445065/posts/pfbid02KcVVET7oJ3nQ87kXckeiCMej53BEzxWqe3e2QUVL6sFagaTTJvAJufY4iagZpJL3l?dco_ad_id=120231631264570528",
    "https://www.facebook.com/100064867445065/posts/pfbid02hg7AzLzCqkNdk3MozJFbS4e2hBgkZ3XtkcR8ed9tpfMeVsEZPoJ63GTiDuRMXvFgl?dco_ad_id=120231631264480528",
    "https://www.facebook.com/100064867445065/posts/pfbid0vQUeAa1X95qwMbjANfn6KPv2P4BdAAZ387ZfHb3yuX2FWLGFSPcBMBhywP2AUrsel?dco_ad_id=120231631264470528",
    "https://www.facebook.com/100064867445065/posts/pfbid02ozQ1UrjCkWeg3kY5x7XEHuFmsPohgkvZhs6vS6nNr3AA9GUpa1gzW8AbcAMXfToKl?dco_ad_id=120231631264460528",
    "https://www.facebook.com/100064867445065/posts/pfbid0s9yrszyccQ26GEXd6s1Q7agRmZ8RZDgfP7A2GUi33qStcjDiBugRneU7HTzjzG3rl?dco_ad_id=120231631264450528",
    "https://www.facebook.com/100064867445065/posts/pfbid0uBP7PsKLcLXb45ZWQnQWgqVsUEiUWQYAZUM3Bd7c3SzBERTBEwhtKrA5DChdR6QJl?dco_ad_id=120232172592140528",
    "https://www.facebook.com/100064867445065/posts/pfbid0VJAHuEFyAuzTRm5fxqGxTcH5LtfbrEExF87mKchBduuFVu7CJJJDBJHwmSajwLYyl?dco_ad_id=120232176271180528",
    "https://www.facebook.com/100064867445065/posts/pfbid0rfayMJkDEYuewJnTjknbZtoRc5ML8Lpu8u4cGZ6U4kW9eVJnKGQ676dDodWDJ1Bgl?dco_ad_id=120232176271170528",
    "https://www.facebook.com/100064867445065/posts/pfbid0QoobJ4yFwFs1g7jDW5aFqJpxdjWCS3nmL7awRyu2TvXHQwHF8BFNWYEQCyLCN42rl?dco_ad_id=120232176271160528",
    "https://www.facebook.com/100064867445065/posts/pfbid0bWt7Je9ZaByYKnXyS9p5y4eHayAZANV2cNxMwAUH8YqeFxJ4zgT28urKKpe1iXMNl?dco_ad_id=120232176271150528",
    "https://www.facebook.com/100064867445065/posts/pfbid0qv1dcSc8pKVF6G8H3mT6AKFoZvGUCvea4Jaw8f3NsvL4XKfpS8nnDgqAN5Ti7HMQl?dco_ad_id=120232176271140528",
    "https://www.facebook.com/100064867445065/posts/pfbid0ro82mHpfHyVekbtHu9XCXTDLDzrEpnZm3wQhaYFW1tfvBizfnRFi1R4Vyj6caNw7l?dco_ad_id=120232176271130528",
    "https://www.facebook.com/100064867445065/posts/pfbid034H5qdwJB4QWzAy1z1ErqdZ74LPrjd7AMzM7HHgS7GhYwuYD2P4jrHaUxcXQcoMQKl?dco_ad_id=120232176271120528",
    "https://www.facebook.com/100064867445065/posts/pfbid0uBP7PsKLcLXb45ZWQnQWgqVsUEiUWQYAZUM3Bd7c3SzBERTBEwhtKrA5DChdR6QJl?dco_ad_id=120232176271110528",
    "https://www.facebook.com/100064867445065/posts/pfbid02XiAfBpvsuWaDtsaQSGtdWLzs8ydC1HEmiiSTYoE8vfwQnT1oB9pw3dNkMLoAECu6l?dco_ad_id=120232176271100528",
    "https://www.facebook.com/100064867445065/posts/pfbid0gXAMN6bRLjQn886Qz9eKbC4Vq1qh7vME1hSvedVF7TvYY4EG67WDpuZ1KwCKCRXKl?dco_ad_id=120232176271090528",
    "https://www.facebook.com/100064867445065/posts/pfbid02SYAed6fygG98yjMiDNPuMhbdvjei8y4gsnWQQwNbduym5jhD9ht1umRXMAXLybSEl?dco_ad_id=120232176271080528",
    "https://www.facebook.com/100064867445065/posts/pfbid02c5ke4RRZaiehrX3ofU1ArU6gd4frEFwf5Q2hbtRD1b12c7HmUbgA6xPvpBBmz57Kl?dco_ad_id=120232176271070528",
    "https://www.facebook.com/100064867445065/posts/pfbid0vPpfJb7yQdDAdGmfWLQW8rK9nEfws7a1UqRPetnRhUDVB2GAFvyS8qyj2qMd4i1wl?dco_ad_id=120232176271060528",
    "https://www.facebook.com/100064867445065/posts/pfbid02mWEtBLUhJYvGc5QarCVp1wFe8CaMXw7QGKdzpFvc4FMrxtjB1tMe2jWf2w9YQkJAl?dco_ad_id=120232176271050528",
    "https://www.facebook.com/100064867445065/posts/pfbid028rFH8142jR7YpKwTRS7zNmeQyXAJcXFSU4gtsbfetLEmTVViX3WnHozKNzEr7mfRl?dco_ad_id=120232176271020528",
    "https://www.facebook.com/100064867445065/posts/pfbid0Tk4ZenxoRcho7NXqkLcMpJXxZcFFyxDhhAjCYzMkYAWM67zNKoBVpJm673QaiX7nl?dco_ad_id=120232176271010528",
    "https://www.facebook.com/100064867445065/posts/pfbid02SDi9VcUxwh4B3teVGyKxHxseC8KA7vyguhavx3RLYaxcX5EdkyGA5Fu672m3jL6Yl?dco_ad_id=120232067268890528",
    "https://www.facebook.com/100064867445065/posts/pfbid02SDi9VcUxwh4B3teVGyKxHxseC8KA7vyguhavx3RLYaxcX5EdkyGA5Fu672m3jL6Yl?dco_ad_id=120232067498230528",
    "https://www.facebook.com/100064867445065/posts/pfbid02eXBnKQQDR2tL4jbftRQ1T1yB6tFCQwtL7gcJHVLNMEVrKTAvDwi8txriaevKMk2Jl?dco_ad_id=120232807081760528",
    "https://www.facebook.com/100064867445065/posts/pfbid0o8pigbaCJHUbuV7BzZVSphQcaX34ErpQ8dqNSLFLeLXAwETx8yuVxuukCQFAFVifl?dco_ad_id=120232550474620528",
     # Continuación URLS_A_PROCESAR - Links 401-500
     # FACEBOOK - Posts (continuación)
    "https://www.facebook.com/100064867445065/posts/pfbid02xkzfHyV3vWddvtnY9UezMnuDhBhs26qyMusc45UaectCzrjKsWC3r6Cv4THfPPSxl?dco_ad_id=120232550474590528",
    "https://www.facebook.com/100064867445065/posts/pfbid0TaTXVV99KiCr1JLgd4gSizC8iJrjDSQRFS1EovpYUVDgiNKU8CefKziSGRqBv36Sl?dco_ad_id=120232550474600528",
    "https://www.facebook.com/100064867445065/posts/pfbid0i4cSqqxT7Cr9ar92THL7M6N2SVF1ur8nYPY2mNJcqQpogcrCjxWRLBbfqVRjmB8ml?dco_ad_id=120232550474610528",
    "https://www.facebook.com/100064867445065/posts/pfbid05HX6U6qLMTpQ5YyQScFWUjQSD7zZcAeRyndapU5pSPM8n3aCV82oy1mCopeewGAml?dco_ad_id=120231631264600528",
    "https://www.facebook.com/100064867445065/posts/pfbid0djeVA7aMiXY8v2mJ1ChibNjPWGUi2xTKKe19AFP6SLfCEQd8D7mdpb6UvcKQDrXXl?dco_ad_id=120231631264480528",
    "https://www.facebook.com/100064867445065/posts/pfbid0335ksK12KXGWr17Q8ghVjzSTAdETzCGxs3q49v4P82zFbxydXEQZqrKrfBWkcFbUYl?dco_ad_id=120233994998930528",
    "https://www.facebook.com/100064867445065/posts/pfbid0Wz31X3mGeVGFyt56sriPX1misXhYawEw3CVq4HeFEcFWrm2J9cpzq6rA85YeokSSl?dco_ad_id=120233995048030528",
    "https://www.facebook.com/100064867445065/posts/pfbid021gHeeGXiEN8N8CriZiG5Rx3VXR5648nsRpsKCE4jTPG133ehduv4YyQgNjtUvYRgl?dco_ad_id=120233995048140528",
    "https://www.facebook.com/100064867445065/posts/pfbid0bR1Kph1sFyuB4GrAX8hXqFkWckcCbhtub3FsDmd4pJXmWGyGvvXx2Fd3vsCRX78kl?dco_ad_id=120233995048180528",
    "https://www.facebook.com/100064867445065/posts/pfbid0fA1MTPg3vx2nySPLnAjvzz92UQrQQcoUPXHmY1kqC791PU5dLe6TXmCJkkZDZLYTl?dco_ad_id=120233995048160528",
    "https://www.facebook.com/100064867445065/posts/pfbid0VxBsjzAmu4HpPXM3FwRPpvjdDXSuLs69WkseSTWTmS7FP6kFbwJjFd8MC2SXoFval?dco_ad_id=120233994697320528",
    "https://www.facebook.com/100064867445065/posts/pfbid029ZzFQtGGsJJnVCFiGWGEyWAJqV4ggfvRZ8ndVYdLEEF3YypCRenBBH7fmqFaoc49l?dco_ad_id=120233991477580528",
    "https://www.facebook.com/100064867445065/posts/pfbid02Kasm7e4R2R82eGsq2v4SVrmtTEpTutTSf9FvbFF7ZeYoLTpevpAziNCTqtfgSxs7l?dco_ad_id=120233995048000528",
    "https://www.facebook.com/100064867445065/posts/pfbid07qBq7ouMaP3Mu9PD7oBAqtMUKWcCLwaQQ9iJNknCLeiBhvZtY6Fsr12ZuquzLvXQl?dco_ad_id=120233995048190528",
    "https://www.facebook.com/100064867445065/posts/pfbid02RzJMU9Z2FXs1mEuA7FHS1mdriCRafkKukpBr1KqZkBm837MbuMzmUcorLLqET2CZl?dco_ad_id=120233995048170528",
    "https://www.facebook.com/100064867445065/posts/pfbid036d5ARFZihQbbi2t4bLw9UVKr1i75YX7BkkhX6w4fdxgiBoLZ5w8ZL3GiUezCsTpul?dco_ad_id=120233995048020528",
    "https://www.facebook.com/100064867445065/posts/pfbid02BxFJcE7rLosUUvneP5LnWg6cs7Xyp94NW74mLkipQwayF3g8yovMMkreWpQcz9rql?dco_ad_id=120233995047970528",
    "https://www.facebook.com/100064867445065/posts/pfbid037VRFKv8vWqjUG3vFPNK7iSZzLGgvGRNtaZqoSm6k9x8C75Q2UnTvfjRuakjWKpwcl?dco_ad_id=120233995047960528",
    "https://www.facebook.com/100064867445065/posts/pfbid0arnjtuNMPbyZLCLwdyQjAbrKCnu2uXMEP4XzhuUbBJXtXLmV8RCU7ThB1c9Y5W7Ql?dco_ad_id=120233995048110528",
    "https://www.facebook.com/100064867445065/posts/pfbid02Y4HQp7RYYi2n6SKEaqSPfeeycGQVo6xY4h1M4K5gFKobcpyovYs4fuCFQZrrUJuJl?dco_ad_id=120233995047980528",
    "https://www.facebook.com/100064867445065/posts/pfbid02Yp5RKP5ApY61nE9wBNJPgCovaMJaiBUkUxcnEF8ck64rGGFrDCrcTnjTBXejLB22l?dco_ad_id=120233993986740528",
    "https://www.facebook.com/100064867445065/posts/pfbid032ahzvKuNR5CVfAyGHNR8GpKaH6Yudz3QQdbQZs3Q1aSPTj9qx1qgG8fbUdbGitZsl?dco_ad_id=120233995048010528",
    "https://www.facebook.com/100064867445065/posts/pfbid02Dnj8qpz7W88WuB2mgzxjyzQJvkw3MABB72i73TWBkVji8Fw8SQiwGdZ97AsBCCdfl?dco_ad_id=120233995048060528",
    "https://www.facebook.com/100064867445065/posts/pfbid02ezWF7mkba97HkLQZbUcutQHTjmJXDFGvjRgouQkep69hRRLHpvJn6WdrXkKu8krnl?dco_ad_id=120233995048150528",
    "https://www.facebook.com/100064867445065/posts/pfbid02vrP6UqtHEDN4JrFPG75g4UYyJ18jYU6MnU2GdX3uWJkaPzCN5XDe5oUQGFi6Hptkl?dco_ad_id=120233995048090528",
    "https://www.facebook.com/100064867445065/posts/pfbid02Qg54qT7EQdiBw59L1hrrxj89pmabaBHWyV98ntaoehniAexFnKiaX9EvsbnN4Mj8l?dco_ad_id=120233994823750528",
    "https://www.facebook.com/100064867445065/posts/pfbid02cm6D4NkZcRmeMTrp9qr8cGDboHhJpPj8HsMQvq9Fd8Y3ZRfvD3TyZQnT5seYjqbwl?dco_ad_id=120233995048120528",
    "https://www.facebook.com/100064867445065/posts/pfbid0339ab9L5Xrc6FnG4vHPkTqZGd1bDA3Zsze356gGQzafgoRhFm4vebZm2ChTH66Xehl?dco_ad_id=120233994823760528",
    "https://www.facebook.com/100064867445065/posts/pfbid0yo5Tqbo5rrf9mSxkv3ry4KJMe1axU25eMuCaDUxrX2yJqLjRqDwbNJCHjCf2aHAZl?dco_ad_id=120233994928910528",
    "https://www.facebook.com/100064867445065/posts/pfbid0YA5Ba5KmBXccNtf3Y9diHqzasjzjs7KT1BwRcwbUzAPHVmkwEbVg3qNvjosgxU3ul?dco_ad_id=120233995048050528",
    "https://www.facebook.com/100064867445065/posts/pfbid0b3pVZNbbA4Z2HB9G5cUpjDFfNrfUie5LXvueMTbKXP7KoECUVghUh41B1MYgnywFl?dco_ad_id=120233994928900528",
    "https://www.facebook.com/100064867445065/posts/pfbid0eyXGhwDgoofmUe9iMJ8ETrrLfueKaa3d1kmanGTfDUCd5xj1M6nq2V7x8uaFr8ozl?dco_ad_id=120233995048080528",
    "https://www.facebook.com/100064867445065/posts/pfbid021MqAx1yoLMgH7hmaaSRbev1tE1zmH8HBv4hMCNCsTvB7qAR6dLQdyBC1r8qgnuTjl?dco_ad_id=120233994705960528",
    "https://www.facebook.com/100064867445065/posts/pfbid031GhRvWFL7TPT9JDPixNGNTkGYUsFubo1XVRoqnhBB7PMtyB2bTvusKBQKhtiZ3mRl?dco_ad_id=120233995048040528",
    "https://www.facebook.com/100064867445065/posts/pfbid0bKDcXx1p4JSZ8b82Voumpk72jG7RUbXjhcN7dZ9D36KyiLENztK1f3vVwxJ1diYgl?dco_ad_id=120233995048100528",
    "https://www.facebook.com/100064867445065/posts/pfbid0viJQBqJ27zMaEC7AFuXyXJ3j6auDbdxQa3JFpSeZ3tTXSG7u7JBotfc7CDyKnyNDl?dco_ad_id=120233995047990528",
    "https://www.facebook.com/100064867445065/posts/pfbid0kSLmf7LyqziF6Exu1E9UAdfVoMb8pZAL9bL2r5XMuFHyDGe3TgphQ2zgotus1kn5l?dco_ad_id=120233995048070528",
    "https://www.facebook.com/100064867445065/posts/pfbid0TxkcgXmtmvDerSR5v5TK213ZL4uH5hikb1gqedRD71qiG4E11n88CoPCWTLQN96Bl?dco_ad_id=120233993986720528",
    "https://www.facebook.com/100064867445065/posts/pfbid0kQ3kcrViMFe45NeyqPviqdw9gdN4Nmx6AKR3tzZKHSNn116HgkBw52CW7KMAU4tUl?dco_ad_id=120233995048130528",
    "https://www.facebook.com/100064867445065/posts/pfbid0uUmWT5we34xuc9Hy3JSdSJDeLbgy99JQBKxXQ2THpj2AKM4YAHngEgt9dzciyckEl?dco_ad_id=120233993986730528",
    "https://www.facebook.com/100064867445065/posts/pfbid02ySeunUm4ppFHBr61Q46Ph6hAJNDiBVpPd8mN5Sf8jTh9sz2g8UjVctJKnm94eJG3l?dco_ad_id=120233994998940528",
    "https://www.facebook.com/100064867445065/posts/pfbid0kSLmf7LyqziF6Exu1E9UAdfVoMb8pZAL9bL2r5XMuFHyDGe3TgphQ2zgotus1kn5l?dco_ad_id=120234668517270528",
    "https://www.facebook.com/100064867445065/posts/pfbid0viJQBqJ27zMaEC7AFuXyXJ3j6auDbdxQa3JFpSeZ3tTXSG7u7JBotfc7CDyKnyNDl?dco_ad_id=120234668516960528",
    "https://www.facebook.com/100064867445065/posts/pfbid037VRFKv8vWqjUG3vFPNK7iSZzLGgvGRNtaZqoSm6k9x8C75Q2UnTvfjRuakjWKpwcl?dco_ad_id=120234668516970528",
    "https://www.facebook.com/100064867445065/posts/pfbid0bKDcXx1p4JSZ8b82Voumpk72jG7RUbXjhcN7dZ9D36KyiLENztK1f3vVwxJ1diYgl?dco_ad_id=120234668517060528",
    "https://www.facebook.com/100064867445065/posts/pfbid02ySeunUm4ppFHBr61Q46Ph6hAJNDiBVpPd8mN5Sf8jTh9sz2g8UjVctJKnm94eJG3l?dco_ad_id=120234668517160528",
    "https://www.facebook.com/100064867445065/posts/pfbid0eyXGhwDgoofmUe9iMJ8ETrrLfueKaa3d1kmanGTfDUCd5xj1M6nq2V7x8uaFr8ozl?dco_ad_id=120234668517120528",
    "https://www.facebook.com/100064867445065/posts/pfbid0yo5Tqbo5rrf9mSxkv3ry4KJMe1axU25eMuCaDUxrX2yJqLjRqDwbNJCHjCf2aHAZl?dco_ad_id=120234668517210528",
    "https://www.facebook.com/100064867445065/posts/pfbid0fA1MTPg3vx2nySPLnAjvzz92UQrQQcoUPXHmY1kqC791PU5dLe6TXmCJkkZDZLYTl?dco_ad_id=120234668517010528",
    "https://www.facebook.com/100064867445065/posts/pfbid02BxFJcE7rLosUUvneP5LnWg6cs7Xyp94NW74mLkipQwayF3g8yovMMkreWpQcz9rql?dco_ad_id=120234668516990528",
    "https://www.facebook.com/100064867445065/posts/pfbid02RzJMU9Z2FXs1mEuA7FHS1mdriCRafkKukpBr1KqZkBm837MbuMzmUcorLLqET2CZl?dco_ad_id=120234668517030528",
    "https://www.facebook.com/100064867445065/posts/pfbid0335ksK12KXGWr17Q8ghVjzSTAdETzCGxs3q49v4P82zFbxydXEQZqrKrfBWkcFbUYl?dco_ad_id=120234668517180528",
    "https://www.facebook.com/100064867445065/posts/pfbid0b3pVZNbbA4Z2HB9G5cUpjDFfNrfUie5LXvueMTbKXP7KoECUVghUh41B1MYgnywFl?dco_ad_id=120234668517170528",
    # FACEBOOK - Videos
    "https://www.facebook.com/100064867445065/videos/759074660165987",
    "https://www.facebook.com/100064867445065/videos/646994368025589",
    "https://www.facebook.com/100064867445065/videos/1448503073025625",
    "https://www.facebook.com/100064867445065/videos/2033891010750740",
    "https://www.facebook.com/100064867445065/videos/1522847888884919",
    "https://www.facebook.com/100064867445065/videos/830303536353287",
    "https://www.facebook.com/100064867445065/videos/1126839739299858",
    "https://www.facebook.com/100064867445065/videos/1151684900311016",
    "https://www.facebook.com/100064867445065/videos/2145723119245215",
    "https://www.facebook.com/100064867445065/videos/1167536955203089",
    "https://www.facebook.com/100064867445065/videos/628841873355679",
    "https://www.facebook.com/100064867445065/videos/796131049564901",
    "https://www.facebook.com/100064867445065/videos/2341765776277589",
    "https://www.facebook.com/100064867445065/videos/801738662198924",
    "https://www.facebook.com/100064867445065/videos/755147344070643",
    "https://www.facebook.com/100064867445065/videos/755847064028606",
    "https://www.facebook.com/100064867445065/videos/783931847511846",
    "https://www.facebook.com/100064867445065/videos/1341628570637720",
    "https://www.facebook.com/100064867445065/videos/789273280272516",
    "https://www.facebook.com/100064867445065/videos/814244691266244",
    "https://www.facebook.com/100064867445065/videos/786596180415875",
    "https://www.facebook.com/100064867445065/videos/2234667103680178",
    "https://www.facebook.com/100064867445065/videos/771357842442287",
    "https://www.facebook.com/100064867445065/videos/717835787956854",
    "https://www.facebook.com/100064867445065/videos/1263741155491594",
    "https://www.facebook.com/100064867445065/videos/4052016791717647",
    "https://www.facebook.com/100064867445065/videos/1939230727004201",
    "https://www.facebook.com/100064867445065/videos/561032720370911",
    "https://www.facebook.com/100064867445065/videos/1306123300908239",
    "https://www.facebook.com/100064867445065/videos/650898810937630",
    "https://www.facebook.com/100064867445065/videos/25142626068674397",
    "https://www.facebook.com/100064867445065/videos/781719031113522",
    "https://www.facebook.com/100064867445065/videos/1377632307302350",
    "https://www.facebook.com/100064867445065/videos/1450312649554599",
    "https://www.facebook.com/100064867445065/videos/1112926276913003",
    "https://www.facebook.com/100064867445065/videos/1197451092404401",
    "https://www.facebook.com/100064867445065/videos/785935173895742",
    "https://www.facebook.com/100064867445065/videos/1272941960983516",
    "https://www.facebook.com/100064867445065/videos/1088181969962745",
    "https://www.facebook.com/100064867445065/videos/1478389826688793",
    "https://www.facebook.com/100064867445065/videos/769549319319000",
    "https://www.facebook.com/100064867445065/videos/1856535798542600",
    "https://www.facebook.com/100064867445065/videos/780194147747053",
    "https://www.facebook.com/100064867445065/videos/762541969866656",
    "https://www.facebook.com/100064867445065/videos/2536452253381951",
    "https://www.facebook.com/100064867445065/videos/798142679303135",
    "https://www.facebook.com/100064867445065/videos/764814619498990",
    "https://www.facebook.com/100064867445065/videos/1504052630621665",
    "https://www.facebook.com/100064867445065/videos/812103241156564",
    "https://www.facebook.com/100064867445065/videos/667415203051455",
    "https://www.facebook.com/100064867445065/videos/1071246621839050",
    "https://www.facebook.com/100064867445065/videos/792626393453208",
     # Links nuevos que no estaban en la lista anterior:
    "https://www.facebook.com/?feed_demo_ad=120234318125910528&h=AQAh5kI5WMn5K0ghYr0",
    "https://www.facebook.com/reel/786596180415875/",
    "https://www.facebook.com/?feed_demo_ad=120234317015340528&h=AQDrOMxQfgBdwsaP8EE",
    "https://www.facebook.com/reel/1341628570637720/",
    "https://www.facebook.com/?feed_demo_ad=120234198744780528&h=AQAEu-V1J3rvOBnhstg",
    "https://www.facebook.com/reel/1167536955203089/",
    "https://www.facebook.com/?feed_demo_ad=120234317977680528&h=AQBvZPv-3bvs8SJ8r1M",
    "https://www.facebook.com/reel/2145723119245215/",
    "https://www.facebook.com/?feed_demo_ad=120234199096330528&h=AQALGqizsR8GVZkE0yo",
    "https://www.facebook.com/reel/796131049564901/",
    "https://www.facebook.com/?feed_demo_ad=120234237275450528&h=AQCo3u5swn6A3SBiMjM",
    "https://www.facebook.com/reel/2341765776277589/",
    "https://www.facebook.com/?feed_demo_ad=120234197141210528&h=AQDc1IVqYETXIgYPPy8",
    "https://www.facebook.com/reel/801738662198924/?v"
]



# INFORMACIÓN DE CAMPAÑA
CAMPAIGN_INFO = {
    'campaign_name': 'CAMPAÑA_MANUAL_MULTIPLE',
    'campaign_id': 'MANUAL_002',
    'campaign_mes': 'Septiembre 2025',
    'campaign_marca': 'TU_MARCA',
    'campaign_referencia': 'REF_MANUAL',
    'campaign_objetivo': 'Análisis de Comentarios'
}

class SocialMediaScraper:
    def __init__(self, apify_token):
        self.client = ApifyClient(apify_token)

    def detect_platform(self, url):
        if pd.isna(url) or not url: return None
        url = str(url).lower()
        if any(d in url for d in ['facebook.com', 'fb.com']): return 'facebook'
        if 'instagram.com' in url: return 'instagram'
        if 'tiktok.com' in url: return 'tiktok'
        return None

    def clean_url(self, url):
        return str(url).split('?')[0] if '?' in str(url) else str(url)

    def fix_encoding(self, text):
        if pd.isna(text) or text == '': return ''
        try:
            text = str(text)
            text = html.unescape(text)
            text = unicodedata.normalize('NFKD', text)
            return text.strip()
        except Exception as e:
            logger.warning(f"Could not fix encoding: {e}")
            return str(text)

    def _wait_for_run_finish(self, run):
        logger.info("Scraper initiated, waiting for results...")
        max_wait_time = 300
        start_time = time.time()
        while True:
            run_status = self.client.run(run["id"]).get()
            if run_status["status"] in ["SUCCEEDED", "FAILED", "TIMED-OUT"]:
                return run_status
            if time.time() - start_time > max_wait_time:
                logger.error("Timeout reached while waiting for scraper.")
                return None
            time.sleep(10)

    def scrape_facebook_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Facebook Post {post_number}: {url}")
            run_input = {"startUrls": [{"url": self.clean_url(url)}], "maxComments": max_comments}
            run = self.client.actor("apify/facebook-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Facebook extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_facebook_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_facebook_comments: {e}")
            return []

    def scrape_instagram_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing Instagram Post {post_number}: {url}")
            run_input = {"directUrls": [url], "resultsType": "comments", "resultsLimit": max_comments}
            run = self.client.actor("apify/instagram-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"Instagram extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} items found.")
            return self._process_instagram_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_instagram_comments: {e}")
            return []

    def scrape_tiktok_comments(self, url, max_comments=500, campaign_info=None, post_number=1):
        try:
            logger.info(f"Processing TikTok Post {post_number}: {url}")
            run_input = {"postURLs": [self.clean_url(url)], "maxCommentsPerPost": max_comments}
            run = self.client.actor("clockworks/tiktok-comments-scraper").call(run_input=run_input)
            run_status = self._wait_for_run_finish(run)
            if not run_status or run_status["status"] != "SUCCEEDED":
                logger.error(f"TikTok extraction failed. Status: {run_status.get('status', 'UNKNOWN')}")
                return []
            items = self.client.dataset(run["defaultDatasetId"]).list_items().items
            logger.info(f"Extraction complete: {len(items)} comments found.")
            return self._process_tiktok_results(items, url, post_number, campaign_info)
        except Exception as e:
            logger.error(f"Fatal error in scrape_tiktok_comments: {e}")
            return []

    def _process_facebook_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['createdTime', 'timestamp', 'publishedTime', 'date', 'createdAt', 'publishedAt']
        for comment in items:
            # <-- CORRECCIÓN: Usando tu bucle for original para máxima compatibilidad
            created_time = None
            for field in possible_date_fields:
                if field in comment and comment[field]:
                    created_time = comment[field]
                    break
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Facebook', 'author_name': self.fix_encoding(comment.get('authorName')), 'author_url': comment.get('authorUrl'), 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': comment.get('repliesCount', 0), 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Facebook comments.")
        return processed

    def _process_instagram_results(self, items, url, post_number, campaign_info):
        processed = []
        # <-- CORRECCIÓN: Usando tu lista de campos de fecha más completa
        possible_date_fields = ['timestamp', 'createdTime', 'publishedAt', 'date', 'createdAt', 'taken_at']
        for item in items:
            comments_list = item.get('comments', [item]) if item.get('comments') is not None else [item]
            for comment in comments_list:
                # <-- CORRECCIÓN: Usando tu bucle for original
                created_time = None
                for field in possible_date_fields:
                    if field in comment and comment[field]:
                        created_time = comment[field]
                        break
                author = comment.get('ownerUsername', '')
                comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'Instagram', 'author_name': self.fix_encoding(author), 'author_url': f"https://instagram.com/{author}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': created_time, 'likes_count': comment.get('likesCount', 0), 'replies_count': 0, 'is_reply': False, 'parent_comment_id': None, 'created_time_raw': str(comment)}
                processed.append(comment_data)
        logger.info(f"Processed {len(processed)} Instagram comments.")
        return processed

    def _process_tiktok_results(self, items, url, post_number, campaign_info):
        processed = []
        for comment in items:
            author_id = comment.get('user', {}).get('uniqueId', '')
            comment_data = {**campaign_info, 'post_url': url, 'post_number': post_number, 'platform': 'TikTok', 'author_name': self.fix_encoding(comment.get('user', {}).get('nickname')), 'author_url': f"https://www.tiktok.com/@{author_id}", 'comment_text': self.fix_encoding(comment.get('text')), 'created_time': comment.get('createTime'), 'likes_count': comment.get('diggCount', 0), 'replies_count': comment.get('replyCommentTotal', 0), 'is_reply': 'replyToId' in comment, 'parent_comment_id': comment.get('replyToId'), 'created_time_raw': str(comment)}
            processed.append(comment_data)
        logger.info(f"Processed {len(processed)} TikTok comments.")
        return processed

def save_to_excel(df, filename):
    try:
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Comentarios', index=False)
            if 'post_number' in df.columns:
                summary = df.groupby(['post_number', 'platform', 'post_url']).agg(Total_Comentarios=('comment_text', 'count'), Total_Likes=('likes_count', 'sum')).reset_index()
                summary.to_excel(writer, sheet_name='Resumen_Posts', index=False)
        logger.info(f"Excel file saved successfully: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving Excel file: {e}")
        return False

def process_datetime_columns(df):
    if 'created_time' not in df.columns: return df
    logger.info("Processing datetime columns...")
    # Intenta convertir todo tipo de formatos (timestamps, ISO, etc.) a un datetime unificado
    df['created_time_processed'] = pd.to_datetime(df['created_time'], errors='coerce', utc=True, unit='s')
    mask = df['created_time_processed'].isna()
    df.loc[mask, 'created_time_processed'] = pd.to_datetime(df.loc[mask, 'created_time'], errors='coerce', utc=True)
    if df['created_time_processed'].notna().any():
        df['created_time_processed'] = df['created_time_processed'].dt.tz_localize(None)
        df['fecha_comentario'] = df['created_time_processed'].dt.date
        df['hora_comentario'] = df['created_time_processed'].dt.time
    return df

def run_extraction():
    logger.info("--- STARTING COMMENT EXTRACTION PROCESS ---")
    if not APIFY_TOKEN:
        logger.error("APIFY_TOKEN not found in environment variables. Aborting.")
        return

    valid_urls = [url.strip() for url in URLS_A_PROCESAR if url.strip()]
    if not valid_urls:
        logger.warning("No valid URLs to process. Exiting.")
        return

    scraper = SocialMediaScraper(APIFY_TOKEN)
    all_comments = []
    post_counter = 0

    for url in valid_urls:
        post_counter += 1
        platform = scraper.detect_platform(url)
        comments = []
        if platform == 'facebook':
            comments = scraper.scrape_facebook_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'instagram':
            comments = scraper.scrape_instagram_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        elif platform == 'tiktok':
            comments = scraper.scrape_tiktok_comments(url, campaign_info=CAMPAIGN_INFO, post_number=post_counter)
        else:
            logger.warning(f"Unknown platform for URL: {url}")
        
        all_comments.extend(comments)
        if not SOLO_PRIMER_POST and post_counter < len(valid_urls):
            logger.info("Pausing for 30 seconds between posts...")
            time.sleep(30)

    if not all_comments:
        logger.warning("No comments were extracted. Process finished.")
        return

    logger.info("--- PROCESSING FINAL RESULTS ---")
    df_comments = pd.DataFrame(all_comments)
    df_comments = process_datetime_columns(df_comments)
    
    final_columns = ['post_number', 'platform', 'campaign_name', 'post_url', 'author_name', 'comment_text', 'created_time_processed', 'fecha_comentario', 'hora_comentario', 'likes_count', 'replies_count', 'is_reply', 'author_url', 'created_time_raw']
    existing_cols = [col for col in final_columns if col in df_comments.columns]
    df_comments = df_comments[existing_cols]

    filename = "Comentarios Campaña.xlsx"
    save_to_excel(df_comments, filename)
    logger.info("--- EXTRACTION PROCESS FINISHED ---")

if __name__ == "__main__":
    run_extraction()












