alter table [dbo].[PR14FinalCSVcreatedbyPython]
  add load_date datetime

UPDATE [dbo].[PR14FinalCSVcreatedbyPython] SET load_date = SYSDATETIME()

UPDATE [nw_staging].[PR14FinalCSVcreatedbyPythonView] SET element_name = 'New element added' where unique_id = 'PR14AFWWSW_W-A1'

UPDATE [dbo].[PR14FinalCSVcreatedbyPython] SET load_date = SYSDATETIME() where unique_id = 'PR14AFWWSW_W-A1'
