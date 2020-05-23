# genesapi-tabular

Export data from GENESIS into tabular format (csv or json)

see [examples (jupyter notebook)](./examples.ipynb)

Example:

[https://tabular.genesapi.org?data=12613:BEV002(NAT,GES)&time=2017&region=01&labels=name](<https://tabular.genesapi.org?data=12613:BEV002(NAT,GES)&time=2017&region=01&labels=name>)

Use the same query with [https://static.tabular.genesapi.org](https://static.tabular.genesapi.org)
as domain to get a never-changing online csv file (via
[genesapi-tabular-static](https://github.com/datenguide/genesapi-tabular-static)):

[https://static.tabular.genesapi.org?data=12613:BEV002(NAT,GES)&time=2017&region=01&labels=name](<https://static.tabular.genesapi.org?data=12613:BEV002(NAT,GES)&time=2017&region=01&labels=name>)

## GET serialisierung von CSV requests

### Regionen auswählen

alle landkreise in brandenburg

    ?region=all&level=3&parent=12

liste von regionen

    ?region=10,12,13

alle bundesländer (default)

    ?region=all&level=1

### Zeitraum festlegen

alle jahre (default)

    ?time=all

zeitraum von-bis

    ?time=2000:2010

zeitraum von anfang bis:

    ?time=:2010

zeitraum seit:

    ?time=2000:

einzelne jahre

    ?time=2000,2004,2012

### Daten auswählen

    ?data=<id>:<attr>(<dim>:<value>,<dim2>:<value2>|<value3>, ...)

Bsp.: einbürgerungstatistik > einbürgerung von ausländern

    ?data=12511:BEV008

einbürgerung von ausländern mit aufenthaltsdauer von unter 8 jahren

    ?data=12511:BEV008(AUFDA1:AUFDA00B08)

einbürgerung von ausländern mit aufenthaltsdauer von unter 8 jahren
oder von 15 bis unter 20 jahren. mehrere argument-werte werden mit `|`
voneinander getrennt.

    ?data=12511:BEV008(AUFDA1:AUFDA00B08|AUFDA15B20)

zweite statistik hinzufügen. einfach einen weiteren `?data` parameter
übergeben:

    ?data=...&data=12612:BEV001

### CSV Export Parameter

csv format einstellen: eine zeile pro wert (default)

    ?layout=long

csv format einstellen: eine zeile pro region

    ?layout=region

csv format einstellen: eine zeile pro jahr

    ?layout=time

csv beschriftung einstellen: nur statistik kürzel (default)

    ?labels=id

csv beschriftung einstellen: ausgeschriebene statistik namen

    ?labels=name

format:

    ?format=csv

    ?format=tsv

    ?format=json  (array of rows)

    ?delimiter=,
