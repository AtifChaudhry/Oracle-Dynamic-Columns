REM
REM Dynamic Columns (DC)
REM
REM Last Modified: 3/27/2012 12:10 PM
REM 

set echo off


create or replace package      UTL_DYNAMIC_COLUMN
is

  function CREATE_NEW
           (c1   varchar2  DEFAULT NULL, c2   varchar2  DEFAULT NULL, 
            c3   varchar2  DEFAULT NULL, c4   varchar2  DEFAULT NULL, 
            c5   varchar2  DEFAULT NULL, c6   varchar2  DEFAULT NULL, 
            c7   varchar2  DEFAULT NULL, c8   varchar2  DEFAULT NULL, 
            c9   varchar2  DEFAULT NULL, c10  varchar2  DEFAULT NULL, 
            c11  varchar2  DEFAULT NULL, c12  varchar2  DEFAULT NULL, 
            c13  varchar2  DEFAULT NULL, c14  varchar2  DEFAULT NULL, 
            c15  varchar2  DEFAULT NULL, c16  varchar2  DEFAULT NULL, 
            c17  varchar2  DEFAULT NULL, c18  varchar2  DEFAULT NULL, 
            c19  varchar2  DEFAULT NULL, c20  varchar2  DEFAULT NULL, 
            c21  varchar2  DEFAULT NULL, c22  varchar2  DEFAULT NULL, 
            c23  varchar2  DEFAULT NULL, c24  varchar2  DEFAULT NULL, 
            c25  varchar2  DEFAULT NULL, c26  varchar2  DEFAULT NULL, 
            c27  varchar2  DEFAULT NULL, c28  varchar2  DEFAULT NULL, 
            c29  varchar2  DEFAULT NULL, c30  varchar2  DEFAULT NULL,
            c31  varchar2  DEFAULT NULL, c32  varchar2  DEFAULT NULL, 
            c33  varchar2  DEFAULT NULL, c34  varchar2  DEFAULT NULL, 
            c35  varchar2  DEFAULT NULL, c36  varchar2  DEFAULT NULL, 
            c37  varchar2  DEFAULT NULL, c38  varchar2  DEFAULT NULL, 
            c39  varchar2  DEFAULT NULL, c40  varchar2  DEFAULT NULL, 
            c41  varchar2  DEFAULT NULL, c42  varchar2  DEFAULT NULL, 
            c43  varchar2  DEFAULT NULL, c44  varchar2  DEFAULT NULL, 
            c45  varchar2  DEFAULT NULL, c46  varchar2  DEFAULT NULL, 
            c47  varchar2  DEFAULT NULL, c48  varchar2  DEFAULT NULL, 
            c49  varchar2  DEFAULT NULL, c50  varchar2  DEFAULT NULL) 
           return varchar2 deterministic; 

  function EXISTS (dyncol varchar2, cid number)               
           return number deterministic;

  function GET    (dyncol varchar2, cid number)               
           return varchar2 deterministic;

  function DELETE (dyncol varchar2, cid number)               
           return varchar2 deterministic;

  function ADD    (dyncol varchar2, cid number, val varchar2) 
           return varchar2 deterministic;

  function LIST   (dyncol varchar2, column_prefix varchar2 default 'C')
           return varchar2 deterministic;

  function JSON   (dyncol varchar2, column_prefix varchar2 default 'C')
           return varchar2 deterministic;

  function XML    (dyncol varchar2, column_prefix varchar2 default 'C')
           return varchar2 deterministic;

end;
/

create or replace package body UTL_DYNAMIC_COLUMN
is
  /* PRIVATE */ 
  type colR is record (cid number, len number, val varchar2(32000), skip boolean);
  type colT is table of colR;
  separator constant char(1) := '.';

  /* PRIVATE */ 
  function PICKLE(cols IN colT) 
           return varchar2
  is
    cids             varchar2(32000) := '';
    lens             varchar2(32000) := '';
    vals             varchar2(32000) := '';
    len              number;
    skip_count       number          := 0;
  begin
    for i in 1 .. cols.count loop

      if (cols(i).skip) then 
        skip_count := skip_count + 1;
        continue; 
      end if;

      cids := cids || cols(i).cid || separator;
      len := length(cols(i).val);
      lens := lens || len  || separator;
      vals := vals || cols(i).val;
    end loop;
    return (cols.count - skip_count) || separator || cids || lens || '$' || vals;
  end;

  /* PRIVATE */ 
  function UNPICKLE(dyncol varchar2, meta_data_only boolean default false) 
           return  colT
  is
    cct  number;
    cols colT := colT();
    str  varchar2(32000);
    val  number := 0;
    pcid number := 0;
    plen number := 0;
    off  number := 0;
  begin
    cct := substr(dyncol, 1, instr(dyncol,separator) - 1);
    cols.extend(cct); 
    for i in 1 .. 2*cct loop
      str := substr(dyncol, instr(dyncol, separator, 1, i) + 1);
      val := substr(str, 1, instr(str, separator) - 1);
      if (val is null) then
        if (i > cct) then 
          val := plen;
          plen := val; 
        else 
          val  := pcid+1;
          pcid := val; 
        end if;
      end if;
      if (i > cct) then cols(i - cct).len := val; else cols(i).cid := val; end if;
    end loop;   

    if (meta_data_only) then return cols; end if;

    off := instr(dyncol, '$') + 1;
    for  i in 1 .. cct loop
      cols(i).val := substr(dyncol, off, cols(i).len);
      off := off + cols(i).len;
    end loop;

    return cols;
  end;

  /* PUBLIC */ 
  function CREATE_NEW
           (c1   varchar2  DEFAULT NULL, c2   varchar2  DEFAULT NULL, 
            c3   varchar2  DEFAULT NULL, c4   varchar2  DEFAULT NULL, 
            c5   varchar2  DEFAULT NULL, c6   varchar2  DEFAULT NULL, 
            c7   varchar2  DEFAULT NULL, c8   varchar2  DEFAULT NULL, 
            c9   varchar2  DEFAULT NULL, c10  varchar2  DEFAULT NULL, 
            c11  varchar2  DEFAULT NULL, c12  varchar2  DEFAULT NULL, 
            c13  varchar2  DEFAULT NULL, c14  varchar2  DEFAULT NULL, 
            c15  varchar2  DEFAULT NULL, c16  varchar2  DEFAULT NULL, 
            c17  varchar2  DEFAULT NULL, c18  varchar2  DEFAULT NULL, 
            c19  varchar2  DEFAULT NULL, c20  varchar2  DEFAULT NULL, 
            c21  varchar2  DEFAULT NULL, c22  varchar2  DEFAULT NULL, 
            c23  varchar2  DEFAULT NULL, c24  varchar2  DEFAULT NULL, 
            c25  varchar2  DEFAULT NULL, c26  varchar2  DEFAULT NULL, 
            c27  varchar2  DEFAULT NULL, c28  varchar2  DEFAULT NULL, 
            c29  varchar2  DEFAULT NULL, c30  varchar2  DEFAULT NULL,
            c31  varchar2  DEFAULT NULL, c32  varchar2  DEFAULT NULL, 
            c33  varchar2  DEFAULT NULL, c34  varchar2  DEFAULT NULL, 
            c35  varchar2  DEFAULT NULL, c36  varchar2  DEFAULT NULL, 
            c37  varchar2  DEFAULT NULL, c38  varchar2  DEFAULT NULL, 
            c39  varchar2  DEFAULT NULL, c40  varchar2  DEFAULT NULL, 
            c41  varchar2  DEFAULT NULL, c42  varchar2  DEFAULT NULL, 
            c43  varchar2  DEFAULT NULL, c44  varchar2  DEFAULT NULL, 
            c45  varchar2  DEFAULT NULL, c46  varchar2  DEFAULT NULL, 
            c47  varchar2  DEFAULT NULL, c48  varchar2  DEFAULT NULL, 
            c49  varchar2  DEFAULT NULL, c50  varchar2  DEFAULT NULL) 
           return varchar2 deterministic
  is
    cols  colT            := colT();
    val   varchar2(32000) := '';
    cid   number          := 0;
    i     number          := 0;
  begin
    cols.extend(50);

    val := c1;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c2;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c3;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c4;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c5;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c6;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c7;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c8;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c9;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;

    val := c10; cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c11;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c12;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c13;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c14;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c15;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c16;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c17;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c18;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c19;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;

    val := c20; cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c21;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c22;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c23;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c24;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c25;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c26;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c27;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c28;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c29;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;

    val := c30; cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c31;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c32;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c33;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c34;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c35;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c36;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c37;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c38;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c39;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;

    val := c40; cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c41;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c42;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c43;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c44;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c45;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c46;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c47;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c48;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;
    val := c49;  cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;

    val := c50; cid := cid + 1;
    if (val is not null) then i:=i+1; cols(i).cid:=cid; cols(i).val:=val; end if;

    if (i = 0) then return null; end if;
    cols.trim(cols.count - i);

    return PICKLE(cols);
  end;

  /* PUBLIC */ 
  function EXISTS(dyncol varchar2, cid number) 
           return number deterministic
  is
    cols colT;
  begin
    if dyncol is null then return null; end if;

    cols := UNPICKLE(dyncol, meta_data_only => true);
    for i in 1 .. cols.count loop
      if cols(i).cid = cid then return 1; end if;
    end loop;

    return 0;
  end;

  /* PUBLIC */ 
  function GET(dyncol varchar2, cid number)               
           return varchar2 deterministic
  is
    cols colT;
    off  number := 0;
  begin
    if dyncol is null then return null; end if;

    cols := UNPICKLE(dyncol, meta_data_only => true);
    for i in 1 .. cols.count loop
      if cols(i).cid = cid then 
        return substr(dyncol, instr(dyncol, '$')+1+off, cols(i).len);
      end if;
      off := off + cols(i).len;
    end loop;

    return null;
  end;

  /* PUBLIC */ 
  function DELETE(dyncol varchar2, cid number)               
           return varchar2 deterministic
  is
    cols colT;
  begin
    if dyncol is null then return null; end if;

    cols := UNPICKLE(dyncol);
    for i in 1 .. cols.count loop
      if cols(i).cid = cid then 
        if cols.count = 1 then return null; end if;
        cols(i).skip := true;
        return PICKLE(cols); 
      end if;
    end loop;

    return dyncol;
  end;

  /* PUBLIC */ 
  function ADD(dyncol varchar2, cid number, val varchar2) 
           return varchar2 deterministic
  is
    cols colT;
  begin
    if dyncol is null then 
      cols := colT(); cols.extend(1);
      cols(1).cid := cid; cols(1).val := val;
      return PICKLE(cols); 
    end if;

    cols := UNPICKLE(dyncol);
    -- If the cid is already present do an update
    for i in 1 .. cols.count loop
      if cols(i).cid = cid then 
        if (val is null) then cols(i).skip := true; else cols(i).val := val; end if;
        return PICKLE(cols); 
      end if;
    end loop;

    -- If the cid is new do an insert (maintaining cid order)
    cols.extend(1);
    for i in 1 .. cols.count loop
      if i = cols.count then 
        cols(i).cid := cid; cols(i).val := val;
        return PICKLE(cols);
      end if;

      if cols(i).cid > cid then
        for j in reverse i .. cols.count loop
          cols(j) := cols(j-1);
        end loop;
        cols(i).cid := cid; cols(i).val := val;
        return PICKLE(cols);
      end if;
    end loop;

    return null;
  end;


  /* PUBLIC */ 
  function LIST(dyncol varchar2, column_prefix varchar2 default 'C')
           return varchar2 deterministic
  is
    cols colT;
    rval varchar2(32000);
  begin
    if dyncol is null then return null; end if;

    cols := UNPICKLE(dyncol);
    rval := column_prefix || cols(1).cid;
    for i in 2 .. cols.count loop
      rval := rval || ', '|| column_prefix ||  cols(i).cid;
    end loop;

    return rval;
  end;

  /* PUBLIC */ 
  function JSON(dyncol varchar2, column_prefix varchar2 default 'C') 
           return varchar2 deterministic
  is
    cols colT;
    rval varchar2(32000) := '';
  begin
    if dyncol is null then return null; end if;
    cols := UNPICKLE(dyncol);
    rval := '{';
    for i in 1 .. cols.count loop
      rval := rval|| '"'|| column_prefix|| cols(i).cid|| '":"'|| cols(i).val|| '"';
      if (i < cols.count) then rval := rval || ', '; end if;
    end loop;
    rval := rval || '}';
    return rval;
  end;

  /* PUBLIC */ 
  function XML(dyncol varchar2, column_prefix varchar2 default 'C') 
           return varchar2 deterministic
  is
    cols colT;
    rval varchar2(32000) := '';
  begin
    if dyncol is null then return null; end if;
    cols := UNPICKLE(dyncol);
    rval := '<XML>';
    for i in 1 .. cols.count loop
      rval := rval || '<' || column_prefix || cols(i).cid || '>' 
                   ||     cols(i).val 
                   || '</'|| column_prefix || cols(i).cid || '>';
    end loop;
    rval := rval || '</XML>';
    return rval;
  end;


end;
/

create or replace function COLUMN_CREATE
    (c1   varchar2  DEFAULT NULL, c2   varchar2  DEFAULT NULL, 
     c3   varchar2  DEFAULT NULL, c4   varchar2  DEFAULT NULL, 
     c5   varchar2  DEFAULT NULL, c6   varchar2  DEFAULT NULL, 
     c7   varchar2  DEFAULT NULL, c8   varchar2  DEFAULT NULL, 
     c9   varchar2  DEFAULT NULL, c10  varchar2  DEFAULT NULL, 
     c11  varchar2  DEFAULT NULL, c12  varchar2  DEFAULT NULL, 
     c13  varchar2  DEFAULT NULL, c14  varchar2  DEFAULT NULL, 
     c15  varchar2  DEFAULT NULL, c16  varchar2  DEFAULT NULL, 
     c17  varchar2  DEFAULT NULL, c18  varchar2  DEFAULT NULL, 
     c19  varchar2  DEFAULT NULL, c20  varchar2  DEFAULT NULL, 
     c21  varchar2  DEFAULT NULL, c22  varchar2  DEFAULT NULL, 
     c23  varchar2  DEFAULT NULL, c24  varchar2  DEFAULT NULL, 
     c25  varchar2  DEFAULT NULL, c26  varchar2  DEFAULT NULL, 
     c27  varchar2  DEFAULT NULL, c28  varchar2  DEFAULT NULL, 
     c29  varchar2  DEFAULT NULL, c30  varchar2  DEFAULT NULL,
     c31  varchar2  DEFAULT NULL, c32  varchar2  DEFAULT NULL, 
     c33  varchar2  DEFAULT NULL, c34  varchar2  DEFAULT NULL, 
     c35  varchar2  DEFAULT NULL, c36  varchar2  DEFAULT NULL, 
     c37  varchar2  DEFAULT NULL, c38  varchar2  DEFAULT NULL, 
     c39  varchar2  DEFAULT NULL, c40  varchar2  DEFAULT NULL, 
     c41  varchar2  DEFAULT NULL, c42  varchar2  DEFAULT NULL, 
     c43  varchar2  DEFAULT NULL, c44  varchar2  DEFAULT NULL, 
     c45  varchar2  DEFAULT NULL, c46  varchar2  DEFAULT NULL, 
     c47  varchar2  DEFAULT NULL, c48  varchar2  DEFAULT NULL, 
     c49  varchar2  DEFAULT NULL, c50  varchar2  DEFAULT NULL) 
                   return varchar2 deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin
  return UTL_DYNAMIC_COLUMN.CREATE_NEW
            (c1,   c2,  c3,  c4,  c5,  c6,  c7,  c8,  c9, c10,
             c11, c12, c13, c14, c15, c16, c17, c18, c19, c20,
             c21, c22, c23, c24, c25, c26, c27, c28, c29, c30,
             c31, c32, c33, c34, c35, c36, c37, c38, c39, c40,
             c41, c42, c43, c44, c45, c46, c47, c48, c49, c50);
end;
/

create or replace function COLUMN_EXISTS(dyncol varchar2, cid number) 
                  return number deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin

  return UTL_DYNAMIC_COLUMN.EXISTS(dyncol, cid);
end;
/

create or replace function COLUMN_GET(dyncol varchar2, cid number) 
                  return varchar2 deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin
  return UTL_DYNAMIC_COLUMN.GET(dyncol, cid);
end;
/

create or replace function COLUMN_DELETE(dyncol varchar2, cid number) 
                  return varchar2 deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin
  return UTL_DYNAMIC_COLUMN.DELETE(dyncol, cid);
end;
/

create or replace function COLUMN_ADD(dyncol varchar2, cid number, val varchar2) 
                  return varchar2 deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin
  return UTL_DYNAMIC_COLUMN.ADD(dyncol, cid, val);
end;
/

create or replace function COLUMN_LIST(dyncol varchar2, 
                                       column_prefix varchar2 default 'C')
                  return varchar2 deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin
  return UTL_DYNAMIC_COLUMN.LIST(dyncol, column_prefix);
end;
/

create or replace function COLUMN_JSON(dyncol varchar2,
                                       column_prefix varchar2 default 'C')
                  return varchar2 deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin
  return UTL_DYNAMIC_COLUMN.JSON(dyncol, column_prefix);
end;
/

create or replace function COLUMN_XML(dyncol varchar2,
                                      column_prefix varchar2 default 'C')
                  return varchar2 deterministic
is
$if dbms_db_version.ver_le_11_2 $then
  -- Do nothing
$else
  PRAGMA UDF;
$end
begin
  return UTL_DYNAMIC_COLUMN.XML(dyncol, column_prefix);
end;
/


set echo on
