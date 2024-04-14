CREATE SCHEMA medical_billing;

CREATE TABLE medical_billing.xml_buffer (xml_content TEXT);

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


CREATE OR REPLACE FUNCTION parse_xml()
RETURNS VOID AS
$$
DECLARE
    xml_row RECORD;
    last_schet_id INT;
    last_sluch_id UUID;
BEGIN
    FOR xml_row IN SELECT xml_content FROM medical_billing.xml_buffer LOOP

        INSERT INTO medical_billing.schet (code_mo, sluch_year, sluch_month, plat, coments)
        SELECT
            (xpath('//CODE_MO/text()', xml_row.xml_content))[1]::VARCHAR,
            (xpath('//YEAR/text()', xml_row.xml_content))[1]::INT,
            (xpath('//MONTH/text()', xml_row.xml_content))[1]::INT,
            (xpath('//PLAT/text()', xml_row.xml_content))[1]::VARCHAR,
            (xpath('//COMENTS/text()', xml_row.xml_content))[1]::TEXT
        RETURNING schet_id INTO last_schet_id;


        INSERT INTO medical_billing.sluch (schet_id, id_sluch, pr_nov, vidpom, moddate, begdate, enddate, mo_custom, lpubase, id_stat, smo, smo_ok)
        SELECT
            last_schet_id,
            (xpath('//ID_SLUCH/text()', xml_row.xml_content))[1]::UUID,
            (xpath('//PR_NOV/text()', xml_row.xml_content))[1]::INT,
            (xpath('//VIDPOM/text()', xml_row.xml_content))[1]::INT,
            (xpath('//MODDATE/text()', xml_row.xml_content))[1]::TIMESTAMP,
            (xpath('//BEGDATE/text()', xml_row.xml_content))[1]::TIMESTAMP,
            (xpath('//ENDDATE/text()', xml_row.xml_content))[1]::TIMESTAMP,
            (xpath('//MO_CUSTOM/text()', xml_row.xml_content))[1]::VARCHAR,
            (xpath('//LPUBASE/text()', xml_row.xml_content))[1]::INT,
            (xpath('//ID_STAT/text()', xml_row.xml_content))[1]::INT,
            (xpath('//SMO/text()', xml_row.xml_content))[1]::VARCHAR,
            (xpath('//SMO_OK/text()', xml_row.xml_content))[1]::INT
        RETURNING sluch_id INTO last_sluch_id;

        INSERT INTO medical_billing.usl (sluch_id, id_usl, code_usl, prvs, dateusl, code_md, skind, typeoper, podr, profil, det, bedprof, kol_usl)
        SELECT
            last_sluch_id,
            (xpath('//ID_USL/text()', xml_row.xml_content))[1]::UUID,
            (xpath('//CODE_USL/text()', xml_row.xml_content))[1]::VARCHAR,
            (xpath('//PRVS/text()', xml_row.xml_content))[1]::INT,
            (xpath('//DATEUSL/text()', xml_row.xml_content))[1]::DATE,
            (xpath('//CODE_MD/text()', xml_row.xml_content))[1]::VARCHAR,
            (xpath('//SKIND/text()', xml_row.xml_content))[1]::INT,
            (xpath('//TYPEOPER/text()', xml_row.xml_content))[1]::INT,
            (xpath('//PODR/text()', xml_row.xml_content))[1]::VARCHAR,
            (xpath('//PROFIL/text()', xml_row.xml_content))[1]::INT,
            (xpath('//DET/text()', xml_row.xml_content))[1]::INT,
            (xpath('//BEDPROF/text()', xml_row.xml_content))[1]::INT,
            (xpath('//KOL_USL/text()', xml_row.xml_content))[1]::INT;
    END LOOP;
END;
$$
LANGUAGE plpgsql;

