CREATE SCHEMA medical_billing;

CREATE TABLE medical_billing.xml_buffer (
xml_content TEXT
);

CREATE TABLE medical_billing.schet (
schet_id SERIAL PRIMARY KEY,
code_mo VARCHAR,
sluch_year INT,
sluch_month INT,
plat VARCHAR,
coments TEXT
);

CREATE TABLE medical_billing.sluch (
sluch_id UUID PRIMARY KEY,
schet_id INT REFERENCES medical_billing.schet(schet_id) ON DELETE CASCADE,
id_sluch UUID,
pr_nov INT,
vidpom INT,
moddate TIMESTAMP,
begdate TIMESTAMP,
enddate TIMESTAMP,
mo_custom VARCHAR,
lpubase INT,
id_stat INT,
smo VARCHAR,
smo_ok INT
);

CREATE TABLE medical_billing.usl (
usl_id UUID PRIMARY KEY,
sluch_id UUID REFERENCES medical_billing.sluch(sluch_id) ON DELETE CASCADE,
id_usl UUID,
code_usl VARCHAR,
prvs INT,
dateusl DATE,
code_md VARCHAR,
skind INT,
typeoper INT,
podr VARCHAR,
profil INT,
det INT,
bedprof INT,
kol_usl INT
);

INSERT INTO medical_billing.xml_buffer (xml_content) VALUES ('<C:\Cstep\sql_middle\C8BAFCEC-B253-4784-A4FA-AE8632F05501.xml>');

CREATE OR REPLACE FUNCTION parse_xml()
RETURNS VOID AS
$$
DECLARE
xml_row RECORD;
BEGIN

FOR xml_row IN SELECT xml_content FROM medical_billing.xml_buffer LOOP
INSERT INTO medical_billing.schet (code_mo, sluch_year, sluch_month, plat, coments)
SELECT
(xpath('//CODE_MO/text()', xml_row.xml_content))[1]::VARCHAR AS code_mo,
(xpath('//YEAR/text()', xml_row.xml_content))[1]::INT AS sluch_year,
(xpath('//MONTH/text()', xml_row.xml_content))[1]::INT AS sluch_month,
(xpath('//PLAT/text()', xml_row.xml_content))[1]::VARCHAR AS plat,
(xpath('//COMENTS/text()', xml_row.xml_content))[1]::TEXT AS coments;

DECLARE last_schet_id INT := currval(pg_get_serial_sequence('medical_billing.schet', 'schet_id'));


INSERT INTO medical_billing.sluch (sluch_id, schet_id, id_sluch, pr_nov, vidpom, moddate, begdate, enddate, mo_custom, lpubase, id_stat, smo, smo_ok)
SELECT
uuid_generate_v4() AS sluch_id,
last_schet_id AS schet_id,
(xpath('//ID_SLUCH/text()', xml_row.xml_content))[1]::UUID AS id_sluch,
(xpath('//PR_NOV/text()', xml_row.xml_content))[1]::INT AS pr_nov,
(xpath('//VIDPOM/text()', xml_row.xml_content))[1]::INT AS vidpom,
(xpath('//MODDATE/text()', xml_row.xml_content))[1]::TIMESTAMP AS moddate,
(xpath('//BEGDATE/text()', xml_row.xml_content))[1]::TIMESTAMP AS begdate,
(xpath('//ENDDATE/text()', xml_row.xml_content))[1]::TIMESTAMP AS enddate,
(xpath('//MO_CUSTOM/text()', xml_row.xml_content))[1]::VARCHAR AS mo_custom,
(xpath('//LPUBASE/text()', xml_row.xml_content))[1]::INT AS lpubase,
(xpath('//ID_STAT/text()', xml_row.xml_content))[1]::INT AS id_stat,
(xpath('//SMO/text()', xml_row.xml_content))[1]::VARCHAR AS smo,
(xpath('//SMO_OK/text()', xml_row.xml_content))[1]::INT AS smo_ok;

DECLARE last_sluch_id UUID := currval(pg_get_serial_sequence('medical_billing.sluch', 'sluch_id'));

INSERT INTO medical_billing.usl (usl_id, sluch_id, id_usl, code_usl, prvs, dateusl, code_md, skind, typeoper, podr, profil, det, bedprof, kol_usl)
SELECT
uuid_generate_v4() AS usl_id,
last_sluch_id AS sluch_id,
(xpath('//ID_USL/text()', xml_row.xml_content))[1]::UUID AS id_usl,
(xpath('//CODE_USL/text()', xml_row.xml_content))[1]::VARCHAR AS code_usl,
(xpath('//PRVS/text()', xml_row.xml_content))[1]::INT AS prvs,
(xpath('//DATEUSL/text()', xml_row.xml_content))[1]::DATE AS dateusl,
(xpath('//CODE_MD/text()', xml_row.xml_content))[1]::VARCHAR AS code_md,
(xpath('//SKIND/text()', xml_row.xml_content))[1]::INT AS skind,
(xpath('//TYPEOPER/text()', xml_row.xml_content))[1]::INT AS typeoper,
(xpath('//PODR/text()', xml_row.xml_content))[1]::VARCHAR AS podr,
(xpath('//PROFIL/text()', xml_row.xml_content))[1]::INT AS profil,
(xpath('//DET/text()', xml_row.xml_content))[1]::INT AS det,
(xpath('//BEDPROF/text()', xml_row.xml_content))[1]::INT AS bedprof,
(xpath('//KOL_USL/text()', xml_row.xml_content))[1]::INT AS kol_usl;
END LOOP;
END;
$$
LANGUAGE plpgsql;
