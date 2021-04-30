  
#create column load_date in table
alter table [dbo].[PR14FinalCSVcreatedbyPython]
  add load_date datetime

#update column load_date with system date
UPDATE [dbo].[PR14FinalCSVcreatedbyPython] SET load_date = GETDATE()

#update any field
UPDATE [nw_staging].[PR14FinalCSVcreatedbyPythonView] SET element_name = '4water' where unique_id = 'PR14AFWWSW_W-A1'

#update load time for that field
UPDATE [dbo].[PR14FinalCSVcreatedbyPython] SET load_date = GETDATE() where unique_id = 'PR14AFWWSW_W-A1'

