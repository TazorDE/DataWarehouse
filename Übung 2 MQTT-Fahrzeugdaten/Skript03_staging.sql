insert into staging.kunde (kunde_id, vorname, nachname, anrede, geschlecht, geburtsdatum, wohnort, quelle)
  values (532985, 'Lukas', 'Himmelsl�ufer', 'Herr', 'm�nnlich', to_date('25.05.19787', 'DD.MM.YYYY'), 1, 'CRM');

insert into staging.fahrzeug (fin, kunde_id, baujahr, modell, quelle)
  values ('WVWIAmVeryRandom', 532985, 2011, 'Seat Leon 1P', 'Fahrzeug DB');

insert into staging.kfzzuordnung (fin, kfz_kennzeichen, quelle)
  values ('WVWIAmVeryRandom', 'DIZ-B 165', 'Fahrzeug DB');