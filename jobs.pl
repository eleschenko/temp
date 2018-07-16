use strict;

use utf8;
use open qw/:std :utf8/;

use CGI;

use XML::LibXML;
use DBI;

  my $cgi = new CGI();
  $cgi::PARAM_UTF8 = 1;

  my %cfg;
  open(CFG,"</var/www/bin/db_connect.cfg");
  while(<CFG>){
	chop;chop;
    $_=~ m/(.*?)=(.*)/i;
    $cfg{"$1"}=$2;
   }
  close (CFG);

  my $dbh = DBI->connect("dbi:Pg:dbname=$cfg{'dbname'};;host=$cfg{'dbhost'};port=$cfg{'dbport'}", "$cfg{'dbuser'}", "$cfg{'dbpasswd'}",{ PrintError => 1, AutoCommit => 0 });

my $job = $ARGV[0];

if ($job eq 'update_tag_name') {

	my $uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	my $la = $1;
	my $dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "$dtime | LA: $la \n";
	my $t1 = `date +%s%N`;

    print "UPDATE tags name ... ";
    my $rv = $dbh->do(qq{
		update tag tg set tag_name = grp.tag
		from (
		     select t2.tag_id, t2.tag
		     from (
		          select t.tag_id,
                  first_value(tag) OVER (partition BY tag_id order by cnt desc nulls last) as tag
		          from (
		               select tag_id, tag, count(*) as cnt
                       from users_coin_tag
		               group by tag_id, tag
                  ) t
             ) t2
		     group by t2.tag_id, t2.tag
		) grp
		where tg.id = grp.tag_id
	}, undef);
	if ($dbh->errstr) {$dbh->rollback();print $dbh->errstr;}
	else {$dbh->commit();print "ok! \n";}


    print "DELETE tags ... ";
	my $rv = $dbh->do(qq{
		delete from tag where id in (
		(select t.id from tag t)
		except
		(select tag_id from users_coin_tag group by tag_id)
		)
	}, undef);
	if ($dbh->errstr) {$dbh->rollback();print $dbh->errstr;}
	else {$dbh->commit();print "ok! \n";}

	print "UPDATE coin_cnt ... ";
	my $rv = $dbh->do(qq{
		update tag set coin_count = t0.cnt
		from (
			select t.tag_id, count(cid) as cnt
			from (
				select tag_id, coin_year_id as cid
				from users_coin_tag
				group by 1, 2
			) t
			group by 1
		) t0
		where tag.id = t0.tag_id
	}, undef);
	if ($dbh->errstr) {$dbh->rollback();print $dbh->errstr;}
	else {$dbh->commit();print "ok! \n";}

#	print "UPDATE view_count=0 for year tags ... ";
#	my $rv = $dbh->do("update tag set view_count = 0 where tag_name ~ ?", undef, '^[0-9]+$');
#	if ($dbh->errstr) {print $dbh->errstr;$dbh->rollback();}
#	else {$dbh->commit();print "ok! \n";}

#	print "UPDATE is_public from all users tags ... ";
#	my $rv = $dbh->do(qq{
#		update users_coin_tag u
#		set is_public = t.is_public
#		from (
#		select id, is_public
#		from users_coin
#		where public_date >= now() - interval '1 month' or edit_date >= now() - interval '1 month'
#		) t
#		where u.users_coin_id = t.id
#	}, undef);
#	if ($dbh->errstr) {$dbh->rollback();print $dbh->errstr;}
#	else {$dbh->commit();print "ok! \n";}

#	print "UPDATE is_public from all site tags ... ";
#	my $rv = $dbh->do(qq{
#		update tag t
#		set is_public = uct.is_public
#		from (
#			 select is_public, tag_id
#			 from users_coin_tag
#			 where is_public > 0
#			 group by is_public, tag_id
#		) uct
#		where t.id = uct.tag_id
#	}, undef);
#	if ($dbh->errstr) {$dbh->rollback();print $dbh->errstr;}
#	else {$dbh->commit();print "ok! \n\n";}

	my $t2 = `date +%s%N`;
	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000));
	print "TOTAL: $t2 sec \n";

}

############# WEEKLY ###############
if ($job eq 'weekly') {
    print "SEARCH WIKI LINKS ... \n\n";
	my $t1 = `date +%s%N`;
	my $sth = $dbh->prepare("select id, url	from wiki");
	$sth->execute();
	while (my $p = $sth->fetchrow_hashref) {
		print $p->{'url'} . ".....";
		my $data = `wget -O - $p->{'url'}`;
		my $public;
		if ($data =~ m/(ucoin|dateconverter)/gim) {print "YES\n\n";$public=1;}
		else {print "NO\n\n";$public=0;}

	    my $rv = $dbh->do(qq{update wiki set is_public = ? where id = ?}, undef, $public, $p->{'id'});
		$dbh->commit();
	}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	print "DONE [$t2 sec] \n";

}

############# COIN PRICE STAT ###############
if ($job eq 'monthly') {
    
	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{
		insert into currency_stat (currency_main_id, euro_value)
		select id, euro_value from currency_main where active = 1
	}, undef);
	$dbh->commit();
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	print "INSERT INTO currency_stat  --> $rv [$t2] \n\n";

	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{
		insert into coin_price_stat (coin_year_id, coin_variety_id, price, chart)
		select id, null as coin_variety_id, price, price_chart
		from coin_year
		where price > 0
		union 
		select coin_year_id, coin_variety_id, price, price_chart
		from coin_year_variety
		where price > 0
	}, undef);
	$dbh->commit();
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	print "INSERT INTO coin_price_stat  --> $rv [$t2] \n\n";


	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{
		insert into users_price_stat 
		select * from users_price where public_date::date = current_date
	}, undef);
	$dbh->commit();
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	print "INSERT INTO users_price_stat  --> $rv [$t2] \n\n";

	my $t1 = `date +%s%N`;
	my $month = $dbh->selectrow_array(qq{
		select to_char(current_date - interval '25 month', 'yyyy-mm')
	}, undef);

	my $path = '/var/www/data/ucoin.net/uploads/msg/' . $month . '';
	`rm -rf $path`;
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	print "REMOVE UPLOAD IMAGES for - $month \n\n";
}

############# SEO YANDEX ###############

if ($job eq 'seo_yandex') {

	# IP 2a01:4f8:10a:acb::2
	print "update table SEO_QUERY \n";
	#print "\n--------------- YANDEX --------------- \n";
	my $t1 = `date +%s%N`;

	my $parser = XML::LibXML->new();

	my $sth=$dbh->prepare(qq{
		select id, query, request_page_y, request_open_y 
		from seo_query 
		where lang_id = 1 
		order by request_open_y, request_date_y desc
		limit 12
	});
	$sth->execute();
	my $errstr = '';
	my $noErr = 1;
	my $qCount = 1;
	while (my $p = $sth->fetchrow_hashref()) {

		my $url = '';
		my $page = ($p->{'request_open_y'}) ? $p->{'request_page_y'} : 1;
		my $pos = 1 + ($page - 1) * 10;

		while ($page < 6 && $noErr && $qCount < 6) {
			print "ID $p->{'id'}: $p->{'query'} -- page: $page \n";
			my $XML = `wget -O - "https://yandex.ru/search/xml?user=roncorb&key=03.18592866:a3ebe0da51054e8fdf9e45316fa73e1a&query=$p->{'query'}&lr=213&l10n=ru&sortby=rlv&filter=none&groupby=attr%3D%22%22.mode%3Dflat.groups-on-page%3D10.docs-in-group%3D1&page=$page"`;
			sleep 2;$qCount++;
			if ($XML =~ /error/) {
				$noErr = 1; 
				print "ERROR\n";
				print "$XML\n";
			} else {
				my $rv = $dbh->do("update seo_query set request_date_y=now(), request_page_y=?, request_open_y=1, xml_y[$page] = ? where id = ?", undef, $page, $XML, $p->{'id'});
				my $xmldoc = $parser->parse_string($XML);
				#my $node = $xmldoc->findnodes("//request/query");
				#my $pos = $node->findvalue('text()');
				my @nodes = $xmldoc->findnodes('//group');
				
				for my $node (@nodes) {
					$url = $node->findvalue('doc/url/text()');
					if ( $url =~ /ucoin.net/ ) {
						print "FIND -- $pos - $url\n\n";
						$page=10;
						my $rv = $dbh->do("update seo_query set pos_y = ?, last_pos_y = pos_y, url_y = ?, request_open_y=0, request_page_y = null where id = ?", undef, $pos, $url, $p->{'id'});
						$errstr .= ($dbh->errstr) ? $dbh->errstr : '';
					} #else {print "$pos - ".$node->findvalue('categ/@name')."\n";}
					$pos++;
				}
				$page++;
			}
		}
		if ($page >= 5 and $page < 10) {
			my $rv = $dbh->do("update seo_query set pos_y = null, last_pos_y = pos_y, url_y = null, request_open_y=0, request_page_y = null where id = ?", undef, $p->{'id'});
			print "-- $page - ZERO \n";
		}

	}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "              ok! [$t2 sec] \n";}

}
############# SEO GOOGLE ###############

if ($job eq 'seo_google') {
	use JSON qw( decode_json );
	use URI::Escape;

	print "\n--------------- GOOGLE --------------- \n";
	my $t1 = `date +%s%N`;

	my $sth=$dbh->prepare('select id, query, lang_id from seo_query where turn <> (select EXTRACT(week from now()))');
	#my $sth=$dbh->prepare('select id, query, lang_id from seo_query where id in (3)');
	$sth->execute();
	my $errstr = '';
	my $q = 1;
	while ( (my $p = $sth->fetchrow_hashref() ) && ($q < 5)) {
		my $pos = 0;
		my $url = '';

		while ($pos < 40) {
			my $hl = ($p->{'lang_id'} == 1) ? 'ru' : 'en';
			my $gl = ($p->{'lang_id'} == 1) ? 'ru' : 'us';
			print "\n----------- $q. $p->{'query'}\n\n";$q++;
			my $json = `wget -O - "https://ajax.googleapis.com/ajax/services/search/web?v=1.0&q=$p->{'query'}&hl=$hl&gl=$gl&rsz=8&start=$pos"`;

			my $google = decode_json($json);

			if ($google) {
				my @results = @{ $google->{'responseData'}{'results'} };
				foreach my $r ( @results ) {
					$pos++;

					$url = $r->{'url'};

					if ( $url =~ /ucoin.net/ && $pos < 100) {

						$url = &url_encode($url);
						my $rv = $dbh->do("update seo_query set pos_g = ?, last_pos_g = pos_g, url_g = ?, turn=(select EXTRACT(week from now())) where id = ?", undef, $pos, $url, $p->{'id'});
						$errstr .= ($dbh->errstr) ? $dbh->errstr : '';
						print "+$pos: URL = " . $url . "\n";
						$pos = 100;
					}
					print "$pos: URL = " . $url . "\n" if ($pos < 100);
					
				}
			} #else {$pos = 100;}
		}
		if ($pos < 100) {
			my $rv = $dbh->do("update seo_query set pos_g = null, last_pos_g = pos_g, url_g = '', turn=(select EXTRACT(week from now())) where id = ?", undef, $p->{'id'});
			print "\n--SAVE----- $q. $p->{'query'}\n\n";
		}
	}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "              ok! [$t2 sec] \n";}
}


############# PRICES ###############

if ($job eq 'ecb') {
	print "update table CURRENCY_MAIN.euro_value \n";
	my $t1 = `date +%s%N`;
	# http://finance.yahoo.com/d/quotes.csv?e=.csv&f=c4l1&s=EURUSD=X,EURRUB=X,EURUAH=X,EURBYR=X
	my $filename = "http://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml";

	my $parser = XML::LibXML->new();
	my $xmldoc = $parser->parse_file($filename);
	my $node;
	my $rate;

	my $sth=$dbh->prepare('select code from currency_main where active = 1');
	$sth->execute();
	my $errstr = '';
	while (my $p = $sth->fetchrow_hashref()) {
		$node = ($xmldoc->findnodes('//*[@currency="' . $p->{'code'} . '"]'))[0];
		$rate = ($node) ? $node->findvalue('@rate') : '';
		if ($rate) {
			print qq~          $p->{'code'} [~;
			my $rv = $dbh->do("update currency_main set euro_value = ?, euro_value_update_date = now() where code = ?", undef, $rate, $p->{'code'});
			$errstr .= ($dbh->errstr) ? $dbh->errstr : '';
			print qq{$rate]\n};
		}
	}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "              ok! [$t2 sec] \n";}
}

if ($job eq 'byr') {

	#BYR
	print "update table CURRENCY_MAIN.euro_value BYR \n";
	my $filename = "http://www.nbrb.by/Services/XmlExRates.aspx";
	if (head($filename)) {
		my $t1 = `date +%s%N`;
		
		my $parser = XML::LibXML->new();
		my $xmldoc = $parser->parse_file($filename);
		my $node = ($xmldoc->findnodes("//Currency/CharCode[text() = 'EUR']"))[0];
		my $rate = ($node) ? $node->findvalue('../Rate') : '';
		my $errstr = '';
		if ($rate) {
			my $rv = $dbh->do("update currency_main set euro_value = ?, euro_value_update_date = now() where code = ?", undef, $rate, 'BYR');
			$errstr .= ($dbh->errstr) ? $dbh->errstr : '';
			print qq~   BYR [BYR/EUR] [$rate]\n~;
		}


		my $t2 = `date +%s%N`;
		$t2 = ($t2 - $t1)/1000000000;
		if ($errstr) {	
			$dbh->rollback();
			print $errstr;
		}
		else {
			$dbh->commit();
			print "              ok! [$t2 sec] \n";
		}
	} else {print "              FILE IS NOT AVAIBLE \n";}

}

if ($job eq 'currency') {

	my $type = $ARGV[1];


	use LWP::Simple;

	print "update table CURRENCY_MAIN.euro_value from GOOGLE spreadsheets \n";
	my $filename = "http://spreadsheets.google.com/feeds/list/0Av2v4lMxiJ1AdE9laEZJdzhmMzdmcW90VWNfUTYtM2c/1/public/basic";
	if (head($filename)) {
		my $t1 = `date +%s%N`;		

		my $parser = XML::LibXML->new();
		my $xmldoc = $parser->parse_file($filename);
		my $node;
		my $rate;

		my $sth=$dbh->prepare('select code from currency_main where active = 1 and (euro_value_update_date is null or euro_value_update_date <> current_date ) and id <> 1');
		$sth->execute();
		my $errstr = '';my $pair = '';
		while (my $p = $sth->fetchrow_hashref()) {
			$pair = "$p->{'code'}";
			$node = ($xmldoc->findnodes('//*[text()="' . $pair . '"]'))[0];
			$rate = 'NoN';
			if ($node) {
				$node = $node->nextSibling();
				$rate = ($node) ? $node->findvalue('text()') : '1';
				$rate =~ /.*: ([,\.\d]+)/;
				$rate = sprintf("%.4f", $1);
				if ($rate > 0) {
					my $rv = $dbh->do("update currency_main set euro_value = ?, euro_value_update_date = now() where code = ?", undef, $1, $p->{'code'});
					$errstr .= ($dbh->errstr) ? $dbh->errstr : '';			
				}
			}
			print qq~   $p->{'code'} [$pair] $rate\n~;
		}

		my $t2 = `date +%s%N`;
		$t2 = ($t2 - $t1)/1000000000;
		if ($errstr) {	
			$dbh->rollback();
			print $errstr;
		}
		else {
			$dbh->commit();
			print "              ok! [$t2 sec] \n";
		}
	} else {print "              FILE IS NOT AVAIBLE \n";}

	print "update table CURRENCY_MAIN.euro_value from GOOGLE floatrates.com \n";
	my $t1 = `date +%s%N`;

	my $parser = XML::LibXML->new();
	my $xmldoc = $parser->parse_file("http://www.floatrates.com/daily/eur.xml");
	my $node;
	my $rate;

	my $sth=$dbh->prepare(qq{select code from currency_main where active = 1 and (euro_value_update_date is null or euro_value_update_date <> current_date ) and id <> 1 order by code});
	$sth->execute();
	my $errstr = '';my $pair = '';
	while (my $p = $sth->fetchrow_hashref()) {
		my $code = $p->{'code'};

		$pair = "EUR/$p->{'code'}";
		$node = ($xmldoc->findnodes("//item/targetCurrency[text() = '$code']"))[0];
		$rate = ($node) ? $node->findvalue("../exchangeRate/text()") : '';
		$rate =~ s/,//;
		$rate = sprintf("%.4f", $rate);
		if ($rate > 0) {
			my $rv = $dbh->do("update currency_main set euro_value = ?, euro_value_update_date = now() where code = ?", undef, $rate, $p->{'code'});
			$errstr .= ($dbh->errstr) ? $dbh->errstr : '';			
		}
		print qq~   $p->{'code'} $rate\n~;
	}

	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	if ($errstr) {	
		$dbh->rollback();
		print $errstr;
	}
	else {
		$dbh->commit();
		print "              ok! [$t2 sec] \n";
	}

#	print "update table CURRENCY_MAIN.euro_value from YAHOO \n";
#	my $rates = $dbh->selectrow_array(qq{select string_agg('"EUR'||code||'"', ',') from currency_main where active = 1 and (euro_value_update_date is null or euro_value_update_date <> current_date )}, undef);
#	my $filename = qq{http://query.yahooapis.com/v1/public/yql?q=select * from yahoo.finance.xchange where pair in ($rates)&env=store://datatables.org/alltableswithkeys};
#	my $try = 0;
#	my $t1 = `date +%s%N`;
#
#	my $parser = XML::LibXML->new();
#
#	while ($try < 9) {
#
#		my $xmldoc = $parser->parse_file($filename);
#		my $xmldoc = ($xmldoc =~ /unavailable/) ? undef : $xmldoc;
#		print "- TRY: $try - \n";
#		if ($xmldoc) {
#		
#			#my $xmldoc = $parser->parse_file($filename);
#			my $node;
#			my $rate;
#
#			my $sth=$dbh->prepare(qq{select code from currency_main where active = 1 and (euro_value_update_date is null or euro_value_update_date <> current_date ) and id <> 1});
#			$sth->execute();
#			my $errstr = '';my $pair = '';
#			while (my $p = $sth->fetchrow_hashref()) {
#				$pair = "EUR/$p->{'code'}";
#				$node = ($xmldoc->findnodes("//rate/Name[text() = '$pair']"))[0];
#				$rate = ($node) ? $node->findvalue("../Rate/text()") : '';
#
#				$rate = sprintf("%.4f", $rate);
#				if ($rate > 0) {
#					my $rv = $dbh->do("update currency_main set euro_value = ?, euro_value_update_date = now() where code = ?", undef, $rate, $p->{'code'});
#					$errstr .= ($dbh->errstr) ? $dbh->errstr : '';			
#				}
#				print qq~   $p->{'code'} [$pair] $rate\n~;
#			}
#
#			my $t2 = `date +%s%N`;
#			$t2 = ($t2 - $t1)/1000000000;
#			if ($errstr) {	
#				$dbh->rollback();
#				print $errstr;
#			}
#			else {
#				$dbh->commit();
#				print "              ok! [$t2 sec] \n";
#				$try = 10;
#			}
#		} else {print "              FILE IS NOT AVAIBLE \n"; sleep 1; $try++;}
#	}

	print "update table CURRENCY_MAIN.euro_value from GOOGLE finance \n";
	my $t1 = `date +%s%N`;
	my $sth=$dbh->prepare(qq{select code from currency_main where active = 1 and (euro_value_update_date is null or euro_value_update_date <> current_date ) and id <> 1 order by code});
	$sth->execute();
	my $errstr = '';my $pair = '';
	while (my $p = $sth->fetchrow_hashref()) {
		my $code = $p->{'code'};
		my $data = `curl --silent "http://finance.google.com/finance/converter?a=1&from=EUR&to=$code"`;
		$data =~ m/class=bld>(.+) /;
		my $rate = $1;
		$rate = sprintf("%.4f", $rate);
		if ($rate > 0) {
			my $rv = $dbh->do("update currency_main set euro_value = ?, euro_value_update_date = now() where code = ?", undef, $rate, $p->{'code'});
			$errstr .= ($dbh->errstr) ? $dbh->errstr : '';			
		}
		print qq~   $p->{'code'} $rate\n~;
	}

	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	if ($errstr) {	
		$dbh->rollback();
		print $errstr;
	}
	else {
		$dbh->commit();
		print "              ok! [$t2 sec] \n";
	}

}

if ($job eq 'prices') {

	my $ttime = 0;

	my $uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	my $la = $1;
	my $dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "$dtime | LA: $la \n";
	print "insert into table COIN_PRICES \t\t\t";
	my $t1 = `date +%s%N`;
	my $errstr = '';
	my $rv = $dbh->do(qq{
		insert into coin_price (coin_year_id, coin_variety_id, users_coin_id, condition_main_id, price_date, price)
		select 
		  (select coin_year_id from users_coin where id = users_coin_id),
		  (select coin_variety_id from users_coin where id = users_coin_id),
          users_coin_id, 
          (select condition_main_id from users_coin where id = users_coin_id), 
          buy_date as price_date,
		  price / (select (select euro_value from currency_main where id = currency_main_id) from users where id = (select users_id from users_coin where id = users_coin_id)) as price
		from users_coin_price
        where is_public = 0 and price > 0 
              and users_id in 
              (
				select id 
				from users u, users_extra ue
				where u.id = ue.users_id and coins > 100 
					and (select 1 from users_ban where users_id = u.id and price_ban = 1) is null
                    and u.last_visit_date > now() - interval '1 week'
			  )
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';

	my $rv2 = $dbh->do(qq{
		update users_coin_price set is_public = 1 where is_public = 0
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';

	my $t2 = `date +%s%N`;
	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $ttime += $t2;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "Count: $rv [$t2 sec] \n\n";}

	sleep 120; # 2 min

	my $uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	my $la = $1;
	my $dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "$dtime | LA: $la \n";
	print "delete old data from COIN_PRICES \t\t";
	my $t1 = `date +%s%N`;
	my $errstr = '';
	my $rv = $dbh->do(qq{
        delete from coin_price where id in 
			(
				select id
				from (
					select id,
						row_number() OVER (PARTITION BY coin_year_id ORDER BY price_date DESC, public_date desc) AS pos
					from coin_price
					where coin_variety_id is null
				) price_w_pos
				where pos > 40   
				union
				select id
				from (
					select id,
						row_number() OVER (PARTITION BY coin_year_id, coin_variety_id ORDER BY price_date DESC, public_date desc) AS pos
					from coin_price
					where coin_variety_id is not null
				) price_w_pos
				where pos > 40                        
			)
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';

	my $t2 = `date +%s%N`;
	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $ttime += $t2;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "Count: $rv [$t2 sec] \n";}

	sleep 60; # 1 min

	$uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	$la = $1;
	$dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "\n$dtime | LA: $la \n";
	print "update table COIN_YEAR.price \t\t\t";
	my $t1 = `date +%s%N`;
	my $errstr = '';

	my $rv = $dbh->do(qq{
		update coin_year set price = null where id in (
			select id from coin_year where price is not null
			except
			select coin_year_id from coin_price
		)
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';

	my $rv = $dbh->do(qq{
		update coin_year cy set price = round(cp.val::numeric,4), price_chart = cp.chart
		from 
        (        
            select coin_year_id,
                case when sum(Cnt) < 30 then sum(Val_Avg) else sum(Val_mediana) end as Val,
                case when sum(Cnt) >= 30 then 1 else 0 end as chart
            from (
                select coin_year_id, avg(Val) as Val_mediana, 0 as Val_Avg, 0 as Cnt
                from (
                    select coin_year_id, 
                        case when busket = 1 then min(price) when busket = 2 then max(price) end as Val
                    from (
                        select coin_year_id, price, price_date, pos,
                            ntile(2) OVER (PARTITION BY coin_year_id ORDER BY price DESC) AS busket
                        from (
                            select coin_year_id, price, price_date,
                                row_number() OVER (PARTITION BY coin_year_id ORDER BY price_date DESC, users_coin_id) AS pos
                            from coin_price
                        ) price_w_pos
                        where pos <=30
                    ) buskets
                    group by coin_year_id, busket
                ) two_rows_in_middle
                group by coin_year_id
                UNION
                select coin_year_id, 0 as Val_mediana, avg(price) as Val_Avg, count(price) as cnt
                from coin_price
                group by coin_year_id
            ) variants
            group by coin_year_id
        ) cp
		where cy.id = cp.coin_year_id and 
		(cy.price is null or 
			(cy.price is not null and (round(cp.val::numeric,4) <> cy.price or cy.price_chart <> cp.chart))
		 )
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';
	
	my $t2 = `date +%s%N`;
	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $ttime += $t2;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "Count: $rv [$t2 sec] \n";}


	sleep 120; # 2 min

	$uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	$la = $1;
	$dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "\n$dtime | LA: $la \n";
	print "update table COIN_TYPE.price_max/min \t\t\t";
	my $t1 = `date +%s%N`;
	my $errstr = '';

	my $rv = $dbh->do(qq{
		update coin_type set price_max = null, price_min = null
        where id not in (
			select coin_type_id from coin_year where price is not null
		) and (price_max is not null or price_min is not null)
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';

	my $rv = $dbh->do(qq{
		update coin_type ct set price_max = t.price_max, price_min = t.price_min
		from (
				 select max(price) as price_max, min(price) as price_min, coin_type_id
				 from coin_year
				 where price is not null
				 group by coin_type_id
		) t
		where ct.id = t.coin_type_id and (
			  ct.price_max is null or ct.price_min is null or 
			  (ct.price_max is not null and (t.price_max <> ct.price_max or t.price_min <> ct.price_min))
		)
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';
	
	my $t2 = `date +%s%N`;
	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $ttime += $t2;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "Count: $rv [$t2 sec] \n";}


	sleep 60; # 1 min

	$uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	$la = $1;
	$dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "\n$dtime | LA: $la \n";
	my $t1 = `date +%s%N`;
	print "update table COIN_YEAR_VARIETY.price \t\t";

	my $rv = $dbh->do(qq{
        update coin_year_variety cy set price = null 
        from 
        (
            select coin_year_id, coin_variety_id from coin_year_variety where price is not null
            except
            select coin_year_id, coin_variety_id from coin_price
        ) t
        where cy.coin_year_id = t.coin_year_id and cy.coin_variety_id = t.coin_variety_id
	}, undef);
	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';

	my $rv = $dbh->do(qq{
		update coin_year_variety cy set price = vp.val, price_chart = vp.chart
		from (
					select coin_year_id, coin_variety_id,
						case when sum(Cnt) < 30 then sum(Val_Avg) else sum(Val_mediana) end as Val,
						case when sum(Cnt) >= 30 then 1 else 0 end as chart
					from (
						select coin_year_id, coin_variety_id, avg(Val) as Val_mediana, 0 as Val_Avg, 0 as Cnt
						from (
							select coin_year_id, coin_variety_id,
								case when busket=1 then min(price) when busket=2 then max(price) end as Val
							from (
								select coin_year_id, coin_variety_id, price, price_date, pos,
									ntile(2) OVER (PARTITION BY coin_year_id, coin_variety_id ORDER BY price DESC) AS busket
								from (
									select coin_year_id, coin_variety_id, price, price_date,
										row_number() OVER (PARTITION BY coin_year_id, coin_variety_id ORDER BY price_date DESC, users_coin_id) AS pos
									from coin_price
									where coin_variety_id is not null
								) price_w_pos
								where pos <= 30
							) buskets
							group by coin_year_id, coin_variety_id, busket
						) two_rows_in_middle
						group by coin_year_id, coin_variety_id
						UNION
						select coin_year_id, coin_variety_id, 0 as Val_mediana, avg(price) as Val_Avg, count(price) as cnt
						from coin_price
						where coin_variety_id is not null
						group by coin_year_id, coin_variety_id
					) variants
					group by coin_year_id, coin_variety_id
				) vp
		where cy.coin_year_id = vp.coin_year_id and cy.coin_variety_id = vp.coin_variety_id and
		 (cy.price is null or 
		   (cy.price is not null and (round(vp.Val::numeric,3) <> round(cy.price::numeric,3) or cy.price_chart <> vp.chart))
		 )
	}, undef);
	$errstr = ($dbh->errstr) ? $dbh->errstr : '';
	
	my $t2 = `date +%s%N`;
	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $ttime += $t2;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "Count: $rv [$t2 sec] \n";}

	sleep 180; # 3 min

#	$uptime = `uptime`;
#	$uptime =~ m/average: (\d+.\d+)/;
#	$la = $1;
#	$dtime = `date '+%H:%M:%S'`; chomp $dtime;
#	print "\n$dtime | LA: $la \n";
#	my $t1 = `date +%s%N`;
#	print "delete USERS_PRICE > 120 days \t\t\t";
#
#	my $rv = $dbh->do(qq{
#		delete from users_price where public_date < now() - interval '120 days'
#	}, undef);
#	$errstr .= ($dbh->errstr) ? $dbh->errstr : '';
#	
#	my $t2 = `date +%s%N`;
#	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $ttime += $t2;
#	if ($errstr) {	$dbh->rollback();print $errstr;}
#	else {$dbh->commit();print "Count: $rv [$t2 sec] \n";}
#
#	sleep 180; # 3 min

	$uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	$la = $1;
	$dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "\n$dtime | LA: $la \n";
	my $t1 = `date +%s%N`;
	print "insert into USERS_PRICE \t\t\t";

	my $rv = $dbh->do(qq{
        insert into users_price (users_id, public_date, user_price_sum, ucoin_price_sum, coins_count, user_price_count, ucoin_price_count, pro)
        select t.id, current_date, sum(my_price),
               round(cast( sum(ucoin_price) *
               (select euro_value from currency_main where id = t.currency_main_id )::NUMERIC  as numeric), 2),
               count(t.id), count(my_price), count(ucoin_price), t.pro
               
        from (
        select u.id, u.pro, u.currency_main_id, ucp.price as my_price, 
               case when is_replica = 1 then 0 
				when uc.coin_variety_id is not null then
                   (select price from coin_year_variety where coin_year_id = uc.coin_year_id and coin_variety_id = uc.coin_variety_id)
               else cy.price end as ucoin_price
        from users u, coin_year cy, users_coin uc left join users_coin_price ucp on uc.id = ucp.users_coin_id
        where u.id = uc.users_id
              and uc.coin_year_id = cy.id
              and (
                  (u.last_visit_date > now() - interval '3 month' and (select coins from users_extra where users_id = u.id) < 500)
                  or u.pro = 1)
        ) t
        group by t.id, t.currency_main_id, t.pro
	}, undef);
	$errstr = ($dbh->errstr) ? $dbh->errstr : '';
	
	my $t2 = `date +%s%N`;
	$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $ttime += $t2;
	if ($errstr) {	$dbh->rollback();print $errstr;}
	else {$dbh->commit();print "Count: $rv [$t2 sec] \n";}

	print "\n\nTotal time:  [$ttime sec] \n\n";

}


############# DAILY ###############

if ($job eq 'daily') {

	my $time = 0; 

	print "update USERS_RATE  \n";
	my $t1 = `date +%s%N`;
	
	print "	delete old rates		";
	my $rv = $dbh->do(qq{
		delete from users_rate where last_date < current_date or (last_date is null and public_date + interval '1 week' < current_date)
	}, undef);
	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows del [$t2 sec] \n";}
	
	print "	insert by age			";
	my $t1 = `date +%s%N`;
	my $rv = $dbh->do(qq{
        insert into users_rate (users_id, discount, age)			
        select id as users_id, 
        case WHEN (pro is null and (pro_last_date is null or (pro_last_date + interval '2 month' < current_date))) then 0.3 else 1 end,
               EXTRACT(YEAR FROM now()) - EXTRACT(YEAR FROM signup_date) as age
        from users
        where last_visit_date + interval '6 month' > now() and
              (EXTRACT(MONTH FROM signup_date) = EXTRACT(MONTH FROM current_date)) and (EXTRACT(DAY FROM signup_date) = EXTRACT(DAY FROM current_date))
			  and (EXTRACT(YEAR FROM current_date) - EXTRACT(YEAR FROM signup_date)) > 1 
              and id not in (select users_id from users_rate)
	}, undef);
	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows ins  [$t2 sec]\n";}

	print "\n";
	print "update SWAP\n";
	my $t1 = `date +%s%N`;
	
	print "	change status to neutral		";
	my $rv = $dbh->do(qq{
		update swap set status = -3, feedback='Automatic status after 6 months', is_wait = 0 where status in (3,4) and main_id in (
		select main_id from (
		select main_id, array_agg(status)::int[] as status, array_agg(id) as ids
		from swap s
		where main_id > 1 and 
			main_id in (select main_id from swap where last_change < now() - interval '6.00 month')
		group by main_id
		) t 
		where 
		  t.status = ARRAY[0,3]
		  or t.status = ARRAY[3,0]
		  or t.status = ARRAY[4,0]  
		  or t.status = ARRAY[0,4]
		  or t.status = ARRAY[4,3]  
		  or t.status = ARRAY[3,4]    
		  or t.status = ARRAY[4,4]
		  or t.status = ARRAY[4,-2]
		  or t.status = ARRAY[-2,4]
		  or t.status = ARRAY[-2,3]
		  or t.status = ARRAY[3,-2]
		  or t.status = ARRAY[-3,4]
		  or t.status = ARRAY[4,-3]
		)
	}, undef);
	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec] \n";}

	print "	cancel swap				";
	my $t1 = `date +%s%N`;
	my $rv = $dbh->do(qq{
		update swap set status = 1, last_change = now(), is_read = 0, is_wait = 0 where main_id in
		(select main_id from swap where status = 5 and last_change < now() - interval '3 day')
	}, undef);
	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}


	print "\n";
	print "delete empty coin translation (30, 31, 34) ... ";
	# 30 - content: coin type
	# 31 - content: coin year
	# 34 - content: coin variety

	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{delete from translation_text where length(text) = 0}, undef);
    my $rv = $dbh->do(qq{
		delete from translation_main where id in (
			select id from (
				select tm.id, key, (select count(*) from translation_text where translation_main_id = tm.id) as cnt
				from translation_main tm
				where translation_block_id in (30, 31, 34)
			) t where t.cnt=0
		)
	}, undef);

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows del  [$t2 sec]\n";}

	print "insert into table COIN_TYPE_TEXT ... ";
	# 30 - content: coin type
	# 31 - content: coin year
	# 34 - content: coin variety

	my $t1 = `date +%s%N`;

    my $rv = $dbh->do(qq{
		insert into coin_type_text (coin_type_id, lang_id, subject, brief)		
		select t.cid, t.lang_id,
			   (select text from v_translation where key = 'subject='||t.cid and lang_id = t.lang_id and block = 'coin_type') as subject,
			   (select text from v_translation where key = 'brief='||t.cid and lang_id = t.lang_id and block = 'coin_type') as brief
		from (
		        select split_part(key,'=', 2)::integer as cid, lang_id
        		from v_translation
		        where block = 'coin_type' and key like 'subject%'
                except
                select coin_type_id as cid, lang_id
		        from coin_type_text
		) t
		where t.lang_id in (select id from lang where is_public = 1)
	}, undef);


	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}

	print "insert into table COIN_YEAR_TEXT ... ";
	# 31 - content: coin year

	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{
		insert into coin_year_text (coin_year_id, lang_id, info)		
		select t.cid, t.lang_id,
			   (select text from v_translation where key = 'info='||t.cid and lang_id = t.lang_id and block = 'coin_year') as info
		from (
		        select split_part(key,'=', 2)::integer as cid, lang_id
        		from v_translation
		        where block = 'coin_year' and key like 'info%'
                except
                select coin_year_id as cid, lang_id
		        from coin_year_text
		) t
		where t.lang_id in (select id from lang where is_public = 1)
	}, undef);

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}

	print "insert into table COIN_VARIETY_TEXT ... ";
	# 34 - content: coin variety

	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{
		insert into coin_variety_text (coin_variety_id, lang_id, description)		
		select t.vid, t.lang_id,
			   (select text from v_translation where key = 'description='||t.vid and lang_id = t.lang_id and block = 'coin_variety') as description
		from (
		        select split_part(key,'=', 2)::integer as vid, lang_id
        		from v_translation
		        where block = 'coin_variety' and key like 'description%'
                except
                select coin_variety_id as vid, lang_id
		        from coin_variety_text
		) t
		where t.lang_id in (select id from lang where is_public = 1)
	}, undef);

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}

	print "\n";
	print "delete empty legends ... ";

	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{delete from legend where id not in (select legend_id from coin_legend)}, undef);

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows del  [$t2 sec]\n";}

	print "\n";
	print "delete messages > 500 items ... ";

	my $t1 = `date +%s%N`;
    my $rv = $dbh->do(qq{
		delete from messages where id in (
		select id
		from (
			select id,
					row_number() OVER (PARTITION BY 
						  case when (to_uid > from_uid) then to_uid else from_uid end,
						  case when (to_uid > from_uid) then from_uid else to_uid end
						  ORDER BY public_date desc) AS pos
			from messages
		) p
		where pos > 500
		)		
	}, undef);

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows del  [$t2 sec]\n";}

	print "\n";
	print "update table USERS.pro ..... ";
	my $t1 = `date +%s%N`;
	my $rv = $dbh->do(qq{
		update users set pro = null where pro = 1 and pro_last_date < now() - interval '1 day'
	}, undef);
	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}

	print "\n";
	print "update table COIN_YEAR.public_ucid				";
	my $t1 = `date +%s%N`;
	
	my $errstr = '';
	my $rv = $dbh->do(qq{
		update coin_year cy set public_ucid = sample_ucid
		where sample_ucid is not null and public_ucid <> sample_ucid
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_year cy set public_ucid = NULL, sample_ucid = null
		where 1 = (select 1 from users_coin where id = cy.public_ucid and is_public not in (1,5) )
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update users_coin set sample_wt = 0 
		where id in (
			  select id from users_coin where sample_wt is null and is_public in (1,5) and is_image1 = 1 and is_image2 = 1
		)
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_year cy set public_ucid = Grp.ucid
		from (
			 select cid, ucid
			 from( 
					 select coin_year_id as cid,
							first_value(uc.id) OVER (partition BY coin_year_id order by sample_wt desc nulls last) as ucid
					 from users_coin uc
					 where is_public in (1,5) and is_image1 = 1 and is_image2 = 1 and coin_year_id not in (select id from coin_year where sample_ucid is not null)
			 ) QQ
			 group by cid, ucid
		) Grp
		where cy.id = Grp.cid
		  and (cy.public_ucid is null or (cy.public_ucid is not null and cy.public_ucid <> Grp.ucid))
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($errstr) {$dbh->rollback();print $errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}

	print "update table COIN_YEAR_VARIETY.public_ucid		";
	my $t1 = `date +%s%N`;

	my $rv = $dbh->do(qq{
		update coin_year_variety cy set public_ucid = NULL
		where 1 = (select 1 from users_coin where id = cy.public_ucid and is_public not in (1,5) )
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_year_variety cy set public_ucid = Grp.ucid
		from (
			 select cid, vid, ucid
			 from( 
					 select coin_year_id as cid, coin_variety_id as vid, 
							first_value(uc.id) OVER (partition BY coin_year_id, coin_variety_id order by sample_wt desc nulls last) as ucid
					 from users_coin uc
					 where is_public in (1,5) and is_image1 = 1 and is_image2 = 1 and coin_variety_id is not null
			 ) QQ
			 group by cid, vid, ucid
		) Grp
		where cy.coin_year_id = Grp.cid and cy.coin_variety_id = Grp.vid
		  and (cy.public_ucid is null or (cy.public_ucid is not null and cy.public_ucid <> Grp.ucid))
	}, undef);
	if ($dbh->errstr) {$errstr = $dbh->errstr;}

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($errstr) {$dbh->rollback();print $errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}


	print "update table COIN_TYPE.public_ucid				";
	my $t1 = `date +%s%N`;

	my $rv = $dbh->do(qq{
		update coin_type ct set public_ucid = NULL, public_code = NULL
		where 1 = (select 1 from users_coin where id = ct.public_ucid and is_public not in (1,5) )
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_type ct set public_ucid = t.public_ucid, 
			public_code = (SELECT (
				SELECT code
				FROM coin_year
				WHERE id = coin_year_id
			  ) AS code
			   FROM users_coin
			   WHERE users_coin.id = t.public_ucid
			 )
		from (
			select uc.coin_type_id, first_value(uc.id) OVER (partition BY uc.coin_type_id order by uc.sample_wt desc nulls last) as public_ucid
			from users_coin uc
			where uc.is_public in (1,5)
		) t
		where ct.id = t.coin_type_id
			and (ct.public_ucid is null or (ct.public_ucid is not null and ct.public_ucid <> t.public_ucid))
	}, undef);
	if ($dbh->errstr) {$errstr = $dbh->errstr;}

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($errstr) {$dbh->rollback();print $errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}

	print "update table period_main.public_ucid				";
	my $t1 = `date +%s%N`;

	my $rv = $dbh->do(qq{
		update period_main set public_ucid = null, public_code = null
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update period_main pm set public_ucid = t.public_ucid, public_code = (select code from coin_year where public_ucid = t.public_ucid)
		from (
		select t1.period_main_id, t1.public_ucid
		from (
			select cy.period_main_id,
				   first_value(public_ucid) OVER (partition BY period_main_id order by uc.sample_wt desc nulls last) as public_ucid
			from coin_year cy, users_coin uc 
			where cy.public_ucid = uc.id
		) t1
		group by 1, 2
		) t
		where t.period_main_id = pm.id
	}, undef);
	if ($dbh->errstr) {$errstr = $dbh->errstr;}

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($errstr) {$dbh->rollback();print $errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}


	print "\n";
	print "update table USERS.region_main_id ... ";
	my $t1 = `date +%s%N`;
	my $rv = $dbh->do(qq{
		update users u set region_main_id = t.region_main_id
		from (
          select users_id, region_main_id
          from (
			select users_id, first_value(region_main_id) OVER (partition BY users_id order by cnt desc) as region_main_id
			from (
			  select users_id, region_main_id, count(*) as cnt
			  from users_coin uc, coin_type ct, region_country rc
			  where uc.coin_type_id = ct.id and ct.country_main_id = rc.country_main_id and rc.is_default=1 
                    and uc.users_id in (select users_id from users_coin where public_date > now() - interval '1 day')
              group by users_id, region_main_id
            ) t0
          ) t1
          group by 1, 2 
        )t
        where u.id = t.users_id and u.region_main_id <> t.region_main_id
	}, undef);
	if ($dbh->errstr) {$errstr = $dbh->errstr;}

	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($errstr) {$dbh->rollback();print $errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}


	#### COIN TYPE SEARCH ####	
	print "\n";
	print "update table coin_type_search ... ";
	my $t1 = `date +%s%N`;

	my $rv = $dbh->do(qq{
		truncate table coin_type_search
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		insert into coin_type_search (coin_type_id, info)
		select t.id, string_agg(coalesce(t.words,' ')::text, ' ')
		from (
		select c.id, lower( string_agg(coalesce(ct.name,' ')::text, ' ') )as words
		from coin_type c, country_text ct
		where c.country_main_id = ct.country_main_id 
		group by c.id 
		union
		select c.id, lower( string_agg(coalesce(ct.name,' ')::text, ' ') )as words
		from coin_type c, country_text ct
		where (select group_id from country_main where id = c.country_main_id) = ct.country_main_id 
		group by c.id 
		union
		select c.id, lower( string_agg(coalesce(vt.name,' ')::text, ' ') )as words
		from coin_type c, value_text vt
		where c.value_main_id = vt.value_main_id 
		group by c.id 
		union
		select c.coin_type_id, lower( string_agg (coalesce(c.year::text,' ')||' '||coalesce(c.year_pattern::text,' ')||' '||coalesce(c.year_local::text,' ')||' '||coalesce(cyt.info,' ')::text, ' '))as words
		from coin_year c left join coin_year_text cyt on (c.id = cyt.coin_year_id)
		group by c.coin_type_id
		union
		select c.id, lower( string_agg (coalesce(ctt.subject,' ') ||' '|| coalesce(ctt.subject,' ')::text, ' '))as words
		from coin_type c, coin_type_text ctt
		where c.id = ctt.coin_type_id and ctt.is_translate = 1
		group by c.id 
		union
		select c.id, lower( string_agg(coalesce(pt.name,' ')::text, ' ') )as words
		from coin_type c, period_text pt
		where c.pids[1] = pt.period_main_id 
		group by c.id 
		union
		select coin_type_id, lower( string_agg(coalesce(l.text,' ') || coalesce(translit,' ')::text, ' ') )as words
		from coin_legend cl, legend l
		where cl.legend_id = l.id
		group by coin_type_id 
		union
		select ct.coin_type_id, lower( string_agg(coalesce(tt.name,' ')::text, ' ') )as words
		from coin_theme ct, theme_text tt
		where ct.theme_main_id = tt.theme_main_id
		group by coin_type_id 
		--union
		--select (select coin_type_id from coin_year where id = uct.coin_year_id), lower( string_agg(coalesce(uct.tag,' ')::text, ' ') )as words
		--from users_coin_tag uct, tag t
		--where uct.tag_id = t.id and t.is_public = 1
		--group by 1 
		) t
		group by t.id
	}, undef);
	if ($dbh->errstr) {$errstr = $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_type_search set info = regexp_replace(info, '⅕', '1/5') where info like '%⅕%'
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_type_search set info = regexp_replace(info, '¼', '1/4') where info like '%¼%'
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_type_search set info = regexp_replace(info, '⅓', '1/3') where info like '%⅓%'
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_type_search set info = regexp_replace(info, '½', '1/2') where info like '%½%'
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_type_search set info = regexp_replace(info, '⅔', '2/3') where info like '%⅔%'
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		update coin_type_search set info = regexp_replace(info, '¾', '3/4') where info like '%¾%'
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}

	my $rv = $dbh->do(qq{
		UPDATE coin_type_search SET info_tsv = to_tsvector(info)
	}, undef);
	if ($dbh->errstr) {$errstr .= $dbh->errstr;}


	if ($rv eq '0E0') {$rv = 0;}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($errstr) {$dbh->rollback();print $errstr;}
	else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}


#### SEO STAT ####
	print "\n";
	print "insert into table SEO_STAT_SUM ... ";
	my $t1 = `date +%s%N`;
	my $rv = $dbh->do(q{
	  insert into seo_stat_sum (cnt, lang_id, source, public_date)
  	  select count(*)as cnt, lang_id, source, date_trunc('day', public_date)
	  from seo_stat_log
	  where date_trunc('day', public_date) = date_trunc('day', now() - interval '1 day')
	  group by lang_id, source, 4
	}, undef);
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {	$dbh->rollback();print "---- ERROR ---- "; print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows ins  [$t2 sec]\n";}

	print "delete from table SEO_STAT_LOG for more then 2 monthes ... ";
	my $t1 = `date +%s%N`;
	my $rv = $dbh->do(qq{delete from seo_stat_log where public_date < now() - interval '2 month'}, undef);
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {	$dbh->rollback();print "---- ERROR ---- "; print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows del  [$t2 sec]\n";}

	print "insert into table SEO_STAT ... ";
	my $rv = $dbh->do(qq{truncate table seo_stat}, undef);
	my $rv = $dbh->do(q{
		insert into seo_stat (url, source, this_month_cnt, last_month_cnt, seo_id, lang_id)
		select t.url, source, sum(t.this_month_cnt), sum(t.last_month_cnt),
		(select id from seo where url = 'https://'||t.url) as seo_id, t.lang_id
		from (
		select s.*
		from (
		select case when (url ~ '/$') then substring(url from '(.+)/') else url end as url, ip, source,
		case when (public_date > now() - interval '1 month') then 1 else 0 end as this_month_cnt,
		case when (public_date > now() - interval '2 month' and public_date < now() - interval '1 month') then 1 else 0 end as last_month_cnt, lang_id
		from seo_stat_log
		where public_date > now() - interval '2 month'
		) s group by url, ip, source, this_month_cnt, last_month_cnt, lang_id
		) t
		group by url, source, lang_id
	}, undef);
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	if ($dbh->errstr) {	$dbh->rollback();print "---- ERROR ---- "; print $dbh->errstr;}
	else {$dbh->commit;print "$rv rows ins  [$t2 sec]\n";}

############# SITEMAP ###############
	print "\n";
	print "Create SITEMAP ... ";
	my $t1 = `date +%s%N`;
	my $xml = qq{<?xml version="1.0" encoding="utf8"?>
		<urlset
		  xmlns="http://www.google.com/schemas/sitemap/0.84"
		  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
		  xsi:schemaLocation="http://www.google.com/schemas/sitemap/0.84
							  http://www.google.com/schemas/sitemap/0.84/sitemap.xsd">};

	$xml .= qq{
	<url>
		  <loc>https://en.ucoin.net</loc>
		  <xhtml:link xmlns:xhtml="http://www.w3.org/1999/xhtml" rel="alternate" href="https://en.ucoin.net" hreflang="en" />
		  <changefreq>daily</changefreq>
		  <priority>1</priority>
	</url>
	<url>
		  <loc>https://en.ucoin.net/catalog</loc>
		  <xhtml:link xmlns:xhtml="http://www.w3.org/1999/xhtml" rel="alternate" href="https://en.ucoin.net/catalog" hreflang="en" />
		  <changefreq>daily</changefreq>
		  <priority>1</priority>
	</url>
	};


	my $sth = $dbh->prepare(qq{
		select code from country_main where id in (
			select country_main_id from coin_type
		)
	  });
	$sth->execute();
	while ( my $p = $sth->fetchrow_hashref ) {
		$xml .= qq{
		<url>
			  <loc>https://en.ucoin.net/catalog/?country=$p->{'code'}</loc>
			  <xhtml:link xmlns:xhtml="http://www.w3.org/1999/xhtml" rel="alternate" href="https://en.ucoin.net/catalog/?country=$p->{'code'}" hreflang="en" />
			  <changefreq>weekly</changefreq>
			  <priority>0.9</priority>
		</url>};
	}


#	my $sth = $dbh->prepare(qq{
#		select (select code from country_main where id = country_main_id) as code, period_main_id as pid, 
#			   (select 1 
#				from coin_type ct, coin_year cy 
#				where ct.id = cy.coin_type_id and 
#					  ct.country_main_id = cp.country_main_id and 
#					  ct.type_main_id = 1 and 
#					  cy.period_main_id = cp.period_main_id
#				limit 1) as t1,
#			   (select 1 
#				from coin_type ct, coin_year cy 
#				where ct.id = cy.coin_type_id and 
#					  ct.country_main_id = cp.country_main_id and 
#					  ct.type_main_id = 2 and 
#					  cy.period_main_id = cp.period_main_id
#				limit 1) as t2,
#			   (select 1 
#				from coin_type ct, coin_year cy 
#				where ct.id = cy.coin_type_id and 
#					  ct.country_main_id = cp.country_main_id and 
#					  ct.type_main_id = 3 and 
#					  cy.period_main_id = cp.period_main_id
#				limit 1) as t3                
#		from country_period cp
#		where is_default = 1 and 1 = (select status from period_main where id = cp.period_main_id)
#	  });
#	$sth->execute();
#	while ( my $p = $sth->fetchrow_hashref ) {
#		if ($p->{'t1'}) {
#			$xml .= qq{
#			<url>
#				  <loc>https://en.ucoin.net/table/?country=$p->{'code'}&amp;period=$p->{'pid'}</loc>
#				  <priority>0.7</priority>
#			</url>};
#		}
#		if ($p->{'t2'}) {
#			$xml .= qq{
#			<url>
#				  <loc>https://en.ucoin.net/table/?country=$p->{'code'}&amp;period=$p->{'pid'}&amp;type=2</loc>
#				  <priority>0.7</priority>
#			</url>};
#		}
#		if ($p->{'t3'}) {
#			$xml .= qq{
#			<url>
#				  <loc>https://en.ucoin.net/table/?country=$p->{'code'}&amp;period=$p->{'pid'}&amp;type=3</loc>
#				  <priority>0.7</priority>
#			</url>};
#		}
#	}

	my $sth = $dbh->prepare(qq{
		select id, code, to_char(edit_date, 'YYYY-MM-DD') as edit_date
		from coin_type ct where is_public = 1 
		  and 1 = (select status from period_main where id = (select period_main_id from coin_year where coin_type_id = ct.id limit 1) limit 1) 

	  });
	$sth->execute();
	while ( my $p = $sth->fetchrow_hashref ) {
		#$p->{'edit_date'} = ($p->{'edit_date'}) ? qq|<lastmod>$p->{'edit_date'}</lastmod>| : '';
		$xml .= qq{
		  <url>
			  <loc>https://en.ucoin.net/coin/$p->{'code'}/?tid=$p->{'id'}</loc>
			  <xhtml:link xmlns:xhtml="http://www.w3.org/1999/xhtml" rel="alternate" href="https://en.ucoin.net/coin/$p->{'code'}/?tid=$p->{'id'}" hreflang="en" />
			  <changefreq>weekly</changefreq>
			  <priority>0.5</priority>
		  </url>};
	}

	$xml .= qq{</urlset>};

	my $sth=$dbh->prepare(q{select id, code from lang where is_public = 1});
	$sth->execute();
	my $path = '';
	while (my $l = $sth->fetchrow_hashref) {
		my $lxml = $xml;
		$lxml =~ s/en\.ucoin/$l->{'code'}\.ucoin/g;
		$lxml =~ s/"en"/"$l->{'code'}"/g;
		$path = '/var/www/data/ucoin.net/sitemap/sitemap-' . $l->{'code'} . '.xml';
		open (FL, ">$path");
		print FL $lxml;
		close (FL);
	}
	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;$time += $t2;
	print "done [$t2 sec]\n";


	print "\nTOTAL: $time sec\n\n";



}

############# HOURLY ###############

if ($job eq 'hourly') {

  my $time = 0;
  my $errstr = '';

	my $uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	my $la = $1;
	my $dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "$dtime | LA: $la \n";


	if ($la < 5) {

		print "CLEAR SESSIONS \n";
		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
		update session set is_use = 0 where is_use = 1 and (
			((session_time <= now() - interval '4 hour') and remember = 0)
			 or
			 ((session_time <= now() - interval '1 month') and remember = 1))
			 and (admin in (0, 1)
		)
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($errstr) {	$dbh->rollback();print $errstr;}
		else {
			$dbh->commit();
			print "        update is_use old session --> $rv [$t2 sec] \n";
		}

		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			delete from session 
			where (session_time <= now() - interval '4 hour' and admin = 2) or (session_time <= now() - interval '6 month')
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($errstr) {	$dbh->rollback();print $errstr;}
		else {
			$dbh->commit();
			print "        delete session --> $rv [$t2 sec] \n\n";
		}


		my $uptime = `uptime`;
		$uptime =~ m/average: (\d+.\d+)/;
		my $la = $1;
		my $dtime = `date '+%H:%M:%S'`; chomp $dtime;
		print "$dtime | LA: $la \n";


		print "CLEAR SWAP \n";
		
		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			delete from swap where main_id in (
				select main_id from (
					select main_id, array_agg(status)::int[] as status, array_agg(id) as ids
					from swap s
					where main_id in (select main_id from swap where last_change < now() - interval '3 month')
					group by main_id
				) t 
				where 
				  t.status = ARRAY[-1,-1]
				  or t.status = ARRAY[-1]
				  or t.status = ARRAY[-1,1]
				  or t.status = ARRAY[1,-1]
				  or t.status = ARRAY[1]
				  or t.status = ARRAY[1,1]
				  or t.status = ARRAY[1,2]
				  or t.status = ARRAY[2,1]
				  or t.status = ARRAY[2,3]
				  or t.status = ARRAY[3,2]
				  or t.status = ARRAY[3,3]
			)
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($errstr) {	$dbh->rollback();print $errstr;}
		else {
			$dbh->commit();
			print "        delete old swaps -->  $rv [$t2 sec] \n\n";
		}



		my $uptime = `uptime`;
		$uptime =~ m/average: (\d+.\d+)/;
		my $la = $1;
		my $dtime = `date '+%H:%M:%S'`; chomp $dtime;
		print "$dtime | LA: $la \n";


		print "	delete old rates - ";
		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			delete from users_rate where last_date < current_date or (last_date is null and public_date + interval '1 week' < current_date)
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = ($t2 - $t1)/1000000000;$time += $t2;
		if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
		else {$dbh->commit;print "$rv rows deleted!  [$t2 sec] \n\n";}


		print "UPDATE USERS_EXTRA \n";
		
		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			update users_extra u set coins = t.cnt, coins_old = coins
			from (
				 select users_id, count(*) as cnt
				 from users_coin
				 group by users_id
			) t
			where u.users_id = t.users_id and u.coins <> t.cnt
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($errstr) {	$dbh->rollback();print $errstr;}
		else {
			$dbh->commit();
			print "        coins -->  $rv [$t2 sec] \n";
		}

		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			update users_extra u set coins_by_year = t.cnt
			from (
				select users_id, count(*) as cnt
				from (
					 select users_id, coin_year_id, coin_variety_id
					 from users_coin
					 where users_id in (select id from users where last_visit_date > now() - interval '2 hour')
					 group by users_id, coin_year_id, coin_variety_id
				) t
				group by users_id

			) t
			where u.users_id = t.users_id and u.coins_by_year <> t.cnt
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {
			$dbh->commit();
			print "        coins_by_year -->  $rv [$t2 sec] \n";
		}


		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			update users_extra u set rank_by_year = t.number_by_year
			from (
				select users_id, row_number() OVER (ORDER BY coins_by_year desc) AS number_by_year
				from users_extra
			) t
			where u.users_id = t.users_id  
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {
			$dbh->commit();
			print "        rank_by_year -->  $rv [$t2 sec] \n";
		}


		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			update users_extra u set coins_by_type = t.cnt
			from (
				select t0.users_id, count(*) as cnt 
                from (
                     select users_id, coin_type_id
                     from users_coin 
                     where users_id in (select id from users where last_visit_date > now() - interval '2 hour')
			         group by users_id, coin_type_id
                ) t0
                group by 1
			) t
			where u.users_id = t.users_id and u.coins_by_type <> t.cnt
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {
			$dbh->commit();
			print "        coins_by_type -->  $rv [$t2 sec] \n";
		}

		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			update users_extra u set rank_by_type = t.number_by_type
			from (
				select users_id, row_number() OVER (ORDER BY coins_by_type desc) AS number_by_type
				from users_extra
			) t
			where u.users_id = t.users_id 
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {
			$dbh->commit();
			print "        rank_by_type -->  $rv [$t2 sec] \n";
		}

		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			update users_extra u set coins_by_country = t.cnt
			from (            
				select users_id, count(*) as cnt from (            
					 select uc.users_id, ct.country_main_id
					 from (
						  select users_id, coin_type_id 
						  from users_coin 
						  where users_id in (select id from users where last_visit_date > now() - interval '2 hour')
						  ) uc, coin_type ct
					 where uc.coin_type_id = ct.id
					 group by users_id, country_main_id
				) t0
				group by users_id
			) t
			where u.users_id = t.users_id and u.coins_by_country <> t.cnt   
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {
			$dbh->commit();
			print "        coins_by_country -->  $rv [$t2 sec] \n";
		}

		my $t1 = `date +%s%N`;
		my $rv = $dbh->do(qq{
			update users_extra u set swap_coins = t.cnt
			from (
			  select s.users_id, sum(s.cnt) as cnt
			  from (
				 select users_id, count(*) as cnt
				 from users_swap
				 group by users_id
				 union all
				 select users_id, 0
				 from users_extra
				 where swap_coins > 0
			   ) s
			   group by s.users_id
			 ) t
			where u.users_id = t.users_id and u.swap_coins <> t.cnt
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = sprintf("%.2f", ( ($t2 - $t1)/1000000000)); $time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {
			$dbh->commit();
			print "        swap_coins -->  $rv [$t2 sec] \n\n";
		}


	  print "insert into USERS_RATE.coins ... ";
	  my $t1 = `date +%s%N`;
	  my $rv = $dbh->do(qq{
			insert into users_rate (users_id, discount, coins)		
			select u.id as users_id, 
			case WHEN (pro is null and (pro_last_date is null or (pro_last_date + interval '2 month' < current_date))) then 0.3 else 1 end, cnt
			from users u, (
			select users_id, coins/1000 * 1000 as cnt
			from users_extra
			where coins/1000 > coins_old / 1000 and coins >= 1000
			) ue
			where u.id = ue.users_id
				  and signup_date + interval '3 month' < current_date
				  and users_id not in (select users_id from users_rate)
		}, undef);
	  if ($rv eq '0E0') {$rv = 0;}
	  my $t2 = `date +%s%N`;
	  $t2 = ($t2 - $t1)/1000000000;$time += $t2;
	  if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
	  else {$dbh->commit();print "$rv rows inserted  [$t2 sec]\n\n";}


	  print "CLEAR SWAP_LIST_LOG ... ";
	  my $t1 = `date +%s%N`;
	  my $rv = $dbh->do(qq{
			delete from swap_list_log where id in (
			select id from (
			select id, row_number() OVER (PARTITION BY owner_uid ORDER BY owner_uid, view_time desc) as pos from
				   swap_list_log
			) t where pos > 12
			)
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = ($t2 - $t1)/1000000000;$time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {$dbh->commit();print "$rv rows delete!  [$t2 sec]\n\n";}


	  print "MESSAGE BLOCK USERS ... ";
	  my $t1 = `date +%s%N`;
	  my $rv = $dbh->do(qq{
		insert into users_ban (users_id, message_ban, message_ban_date, message_ban_reason, message_ban_uid)
		select from_uid, 1, now(), 'Automatic block system', 1 from (
		select from_uid, count(*) as cnt
		from messages
		where public_date > now() - interval '3 hours' and from_uid > 1
		group by from_uid, text
		) t 
		where t.cnt > 60 and (select message_ban from users_ban where users_id = t.from_uid) is null 
		order by t.cnt desc
		}, undef);
		if ($rv eq '0E0') {$rv = 0;}
		my $t2 = `date +%s%N`;
		$t2 = ($t2 - $t1)/1000000000;$time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {$dbh->commit();print "$rv rows inserted!  [$t2 sec]\n\n";}
		if ($rv > 0) {
			my $mail_prog = '/usr/sbin/sendmail';
			open (MAIL, "|$mail_prog -t");
			print MAIL "Content-type: text/html; charset=UTF-8\n";
			print MAIL "To: <info\@ucoin.net>\n";
			print MAIL "From: uCoin.net <noreply\@ucoin.net>\n";
			print MAIL "Subject: BAN - Message $rv spamer(s) \n";
			print MAIL "\n";
			print MAIL "BAN - Message $rv spamer(s)<br/><a href='https://en.ucoin.net/admin/users_ban/?ban=message&func=index' target='_blank'>Automatic block system</a>\n";
			close (MAIL);				
		}


	  print "SWAP BLOCK USERS ... ";
	  my $t1 = `date +%s%N`;
	  my $sth=$dbh->prepare(qq{
		select to_char(public_date, 'dd.mm.yy hh:mi:ss') as date, from_uid, to_uid, text, marker, users_id, 
		(select publicname from users where id = from_uid) as from_publicname,
		(select publicname from users where id = to_uid) as to_publicname,
		(select publicname from users where id = users_id) as publicname
		from messages, swap_address_check 
		where lower(text) like '%'|| lower(marker) ||'%'
		and public_date > now() - interval '1 hour'
		order by public_date desc
	  }, undef);
	  my $message = '';my $count = 0;
	  $sth->execute();
	  while(my $p = $sth->fetchrow_hashref)  {
			$message .= qq{
				Date: $p->{'date'}<br/>
				From UID: <a href="https://en.ucoin.net/uid$p->{'from_uid'}">$p->{'from_publicname'}</a> [$p->{'from_uid'}]<br/>
				To UID: <a href="https://en.ucoin.net/uid$p->{'to_uid'}">$p->{'to_publicname'}</a> [$p->{'to_uid'}]<br/>
				Text:<bt/> $p->{'text'}<br/>
				Marker: $p->{'marker'}<br/>
				Marker owner: $p->{'publicname'} ($p->{'users_id'})<br/>
				---------------------------------------------------------<br/><br/>
			};
			$count++;
		}

		my $t2 = `date +%s%N`;
		$t2 = ($t2 - $t1)/1000000000;$time += $t2;
		if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
		else {print "$count selected!  [$t2 sec]\n\n";}
		if ($count > 0) {
			my $mail_prog = '/usr/sbin/sendmail';
			open (MAIL, "|$mail_prog -t");
			print MAIL "Content-type: text/html; charset=UTF-8\n";
			print MAIL "To: <info\@ucoin.net>\n";
			print MAIL "From: uCoin.net <noreply\@ucoin.net>\n";
			print MAIL "Subject: BAN - Swap $count markers \n";
			print MAIL "\n";
			print MAIL "$message\n";
			close (MAIL);				
		}


		print "\nTOTAL: $time sec\n\n";
	} else {print "\nNO ACTION\n\n";}

}

############# 5min ###############

if ($job eq '5min') {

	my $uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	my $la = $1;
	my $dtime = `date '+%H:%M:%S'`; chomp $dtime;
	print "$dtime | LA: $la \n";

	if ($la < 7) {

	  print "DELETE USERS_LOG < 10 min	--->	";
	  my $t1 = `date +%s%N`;
	  my $rv = $dbh->do(qq{
		delete from users_log where last_visit < now() - interval '10 minutes'
	  }, undef);
	  if ($rv eq '0E0') {$rv = 0;}
	  my $t2 = `date +%s%N`;
	  $t2 = ($t2 - $t1)/1000000000;
	  if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	  else {$dbh->commit;print "$rv rows del  [$t2 sec]\n";}

	  print "UPDATE USERS.last_visit_date	--->	";
	  my $t1 = `date +%s%N`;
	  my $rv = $dbh->do(qq{
		UPDATE users u SET last_visit_date = t.time, ip = t.ip
		FROM (
		select users_id, max(last_visit) as time, ip
		from users_log
		group by users_id, ip
		) t
		WHERE u.id = t.users_id
	  }, undef);
	  if ($rv eq '0E0') {$rv = 0;}
	  my $t2 = `date +%s%N`;
	  $t2 = ($t2 - $t1)/1000000000;
	  if ($dbh->errstr) {$dbh->rollback;print $dbh->errstr;}
	  else {$dbh->commit;print "$rv rows upd  [$t2 sec]\n";}

  	} else {print "\nNO ACTION\n\n";}

	my $com = q~sudo cat /var/log/apache2/ucoin.net.access.log |grep `date +%d/%b/%Y:%H` |awk {'print $1'}|sort|uniq -c|sort -rn|head -n 10~;
	my $ex = `$com`;
	my @ip = split /\n/, $ex;

	foreach my $item (@ip) {
		$item =~ s/^(\s+)(.*)/$2/;
		my ($cnt, $ip) = split /\s/, $item;
		if ($cnt > 2500) {
			my $whois = `whois $ip`;
			$whois =~ m/(.*)(OrgName:\s)(.*)$/m;
			my $comp = $3;
			unless ($comp) {$whois =~ m/(.*)(descr:\s)(.*)$/m; $comp = $3;}

			if (lc($comp) !~ /yandex|google|archive/) {
				my $mail_prog = '/usr/sbin/sendmail';
				open (MAIL, "|$mail_prog -t");
				print MAIL "Content-type: text/html; charset=UTF-8\n";
				print MAIL "To: <info\@ucoin.net>\n";
				print MAIL "From: uCoin.net <noreply\@ucoin.net>\n";
				print MAIL "Subject: DOWNLOAD - $cnt - $ip ($comp) \n";
				print MAIL "\n";
				print MAIL "DOWNLOAD - $cnt - $ip - $comp<br/><a href='https://en.ucoin.net/admin/ip_whois/?view=hour' target='_blank'>Automatic block system</a>\n";
				close (MAIL);
			}
		}
	}

}

if ($job eq 'clear_users_event') {
    print "CLEAR USERS EVENT OLDER 1 WEEK ... ";

    my $rv = $dbh->do(qq{delete from users_event where public_date + interval '1 week' < now()}, undef);

    if ($dbh->errstr) {
		$dbh->rollback();
		print $dbh->errstr;
	}
	else {
		$dbh->commit();
		print "ok! \n\n";
	}
}

if ($job eq 'notification') {

	my $uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	my $la = $1;
	print "LA: $la \n\n";

	my $t1 = `date +%s%N`;
	my %cfg;
	my $query = qq{
		select key, text, lang_id
		from v_translation
		where key in ('domain', 'ucoin-title', 'index', 'settings_notifications', 'messages', 'swap', 'new-notification')
	};
	my $sth=$dbh->prepare($query);
	$sth->execute();
	while (my @row = $sth->fetchrow_array) {
		$cfg{"$row[0]"}[$row[2]] = $row[1];
	}

	my $sth=$dbh->prepare(q{
		select id, email, publicname, lang_id
		from users u
		where u.id in (
		
		select to_uid
		from messages
		where to_uid > 1 and is_read = 0 and is_mail = 0 and
		1 = (select 1 from users where id = to_uid and notify_messages_email = 1)
		union
		select to_uid
		from swap
		where is_read = 0 and is_mail = 0 and
		1 = (select 1 from users where id = to_uid and notify_swap_email = 1)		
		
		) 
		and now() > last_visit_date + interval '30 minute'
	});
	$sth->execute();
	my $count = 0; 
	while (my $p = $sth->fetchrow_hashref) {
		my $domain = lc($cfg{'domain'}[$p->{'lang_id'}]);

		my $text = qq{
			<div style="width:100%;background: none repeat scroll 0 0 #4B6999;color:#FFFFFF;font-size:12px;height:20px;padding:6px 0 0px 10px;">$cfg{'ucoin-title'}[$p->{'lang_id'}]</div>
			<div style="width:100%;padding:10px;">

				<h1 style="border-bottom: 1px solid #d8dfe6;color: #304d77;font-size: 14px;margin: 10px 0 15px;padding: 0 0 4px;">$cfg{'new-notification'}[$p->{'lang_id'}]</h1>
		};

		my $sth=$dbh->prepare(q{
			select to_char(t.pdate, 'Mon DD, YYYY'), u.id, publicname, loc_placename,
				   (select name from country_text where country_main_id = u.country_main_id and lang_id = ?) as country,
				   case when avatar_location is not null then avatar_location else 'no-avatar.jpg' end as avatar,
				   t.cnt1, t.cnt2
			from (		   
				select max(pdate) as pdate, uid, sum(cnt1) as cnt1, sum(cnt2) as cnt2
				from (
					select max(public_date) as pdate, from_uid as uid, count(*) as cnt1, 0 as cnt2
					from messages
					where to_uid = ? and is_read = 0
					group by from_uid
					UNION
					select last_change as pdate, from_uid as uid, 0 as cnt1, 1 as cnt2
					from swap
					where to_uid = ? and (is_read = 0 or is_wait = 1)	
				) t0
				group by uid
			) t, users u
			where u.id = t.uid
			order by t.pdate desc
		});
		$sth->execute($p->{'lang_id'}, ($p->{'id'}) x 2);my $cnt = 0; 
		while (my $u = $sth->fetchrow_hashref) {
			$u->{'country'} = qq|$u->{'loc_placename'}, $u->{'country'}| if($u->{'loc_placename'}); 
			$u->{'country'} = 'Support' if($u->{'id'} == 1);
			$text .= qq{
				<table style="margin:10px 20px;"><tr>
					<td style="vertical-align:top;">
						<a href="https://$domain/uid$u->{'id'}" border="0" title=""><img src="https://i.ucoin.net/avatar/$u->{'avatar'}" style="max-width:60px;max-height:60px;" alt="" /></a>
					</td>
					<td style="padding-left:10px;vertical-align:top;">
						<div><a href="https://$domain/uid$u->{'id'}" style="color: #3b7bea;font-size: 15px;">$u->{'publicname'}</a>
						<span style="color: #666;font-size: 13px;">| $u->{'country'}</span></div>
			};
			if ($u->{'cnt1'}) {
				$text .= qq{
							<a target="_blank" style="width:80px;display:inline-block;padding:6px 16px 6px;margin-top:10px;text-decoration:none;color:#333;border: 1px solid rgba(0, 0, 0, 0.2);background-color:#fff;font-size: 13px;text-align:center;" href="https://$domain/messages/"> $cfg{'messages'}[$p->{'lang_id'}] ($u->{'cnt1'}) </a>
				};
			}
			if ($u->{'cnt2'}) {
				$text .= qq{
							<a target="_blank" style="width:80px;display:inline-block;padding:6px 16px 6px;margin:10px;text-decoration:none;color:#333;border: 1px solid rgba(0, 0, 0, 0.2);background-color:#fff;font-size: 13px;text-align:center;" href="https://$domain/swap-mgr/"> $cfg{'swap'}[$p->{'lang_id'}]</a>
				};
			}
			$text .= qq{						
					</td>
				</tr></table>
			};
			$cnt += $u->{'cnt1'} + $u->{'cnt2'};
		}
				
		$text .= qq{
			</div>
			<div style="width:100%;background: none repeat scroll 0 0 #4B6999;color:#FFFFFF;font-size:12px;height:21px;padding:5px 0px 0px 10px;">&copy; 2007-2017 uCoin.net
					<div style="float:right;margin-right: 20px;">
						<a href="https://$domain/" style="color:#FFFFFF;">$cfg{'index'}[$p->{'lang_id'}]</a>&#160;&#160;|&#160;&#160;<a href="https://$domain/messages/" style="color:#FFFFFF;">$cfg{'messages'}[$p->{'lang_id'}]</a>&#160;&#160;|&#160;&#160;<a href="https://$domain/swap-mgr/" style="color:#FFFFFF;">$cfg{'swap'}[$p->{'lang_id'}]</a>&#160;&#160;|&#160;&#160;<a href="https://$domain/Settings/?v=notifications" style="color:#FFFFFF;">$cfg{'settings_notifications'}[$p->{'lang_id'}]</a>
					</div>
			</div>
		};
	
		my $mail_prog = '/usr/sbin/sendmail';
		my $subject = $cfg{'new-notification'}[$p->{'lang_id'}];
		open (MAIL, "|$mail_prog -t");
		print MAIL "Content-type: text/html; charset=UTF-8\n";
		print MAIL "To: <$p->{'email'}>\n";
		print MAIL "From: uCoin.net <noreply\@ucoin.net>\n";
		print MAIL "Subject: $subject ($cnt)\n";
		print MAIL "\n";
		print MAIL "$text\n";
		close (MAIL);

		$count++;
		print $count . ". " . $p->{'publicname'} . " ($cnt)\n";
	}

	my $rv = $dbh->do(qq{
		update messages set is_mail = 1 where to_uid > 1 and is_read = 0 and is_mail = 0
	}, undef);
	if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
	else {$dbh->commit();print "$rv messages mailed! \n\n";}

	my $rv = $dbh->do(qq{
		update swap set is_mail = 1 where is_read = 0 and is_mail = 0
	}, undef);
	if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
	else {$dbh->commit();print "$rv swap mailed! \n\n";}

	my $uptime = `uptime`;
	$uptime =~ m/average: (\d+.\d+)/;
	my $la = $1;

	my $t2 = `date +%s%N`;
	$t2 = ($t2 - $t1)/1000000000;
	print "LA: $la | Total time: $t2 sec \n\n";


}


if ($job eq 'subscribe') {

	# Find broken emails
	# cat /var/log/mail.log |grep status= >/var/www/ucoin/data/mail.log
	# cat /var/log/mail.log |grep status=deferred |grep -v @ucoin |awk {'print $7'} >/var/www/ucoin/data/mail_deferred.log
	# cat /var/log/mail.log |grep status=bounced |grep -v @ucoin |awk {'print $7'} >/var/www/ucoin/data/mail_bounced.log


	my $ID = $ARGV[1];
	my $uid = $ARGV[2] || 1;

	my ($uptime, $la);

	my $curdate = `date +%d%m%Y_%H%M`;chomp $curdate;
	open(IMG, ">/var/www/data/ucoin.net/temp/news_$curdate.log");
	print "NEWS_ID = $ID\n";
	print "START_UID = $uid\n";
	print IMG "NEWS_ID = $ID\n";
	print IMG "START_UID = $uid\n";

	my $rv = $dbh->do(qq{
		update users set notify_news_email = 0 
		where notify_news_email = 1 
			and last_visit_date < now() - interval '3 month'
	}, undef);
	if ($dbh->errstr) {	$dbh->rollback();print $dbh->errstr;}
	else {
		$dbh->commit();
		print "Clear $rv user emails who was online less then 3 monthes ago! \n\n";
		print IMG "Clear $rv user emails who was online less then 6 monthes ago! \n\n";
	}



	my %cfg;
	my $query = qq{
		select key, text, lang_id
		from v_translation
		where key in ('domain', 'ucoin-title', 'index', 'news', 'unsubscribe', 'news_email')
	};
	my $sth=$dbh->prepare($query);
	$sth->execute();
	while (my @row = $sth->fetchrow_array) {
		$cfg{"$row[0]"}[$row[2]] = $row[1];
	}

	my $query = qq{
		select news_main_id, lang_id, title, description, 
		(select link from news_main where id = news_main_id)
		from news_text 
		where news_main_id = ?
	};
	my $sth=$dbh->prepare($query);
	$sth->execute($ID);
	while (my @row = $sth->fetchrow_array) {
		$cfg{"header"}[$row[1]] = $row[2];
		$cfg{"description"}[$row[1]] = $row[3];
		$cfg{"link"}[$row[1]] = $row[4];
	}

	my $sth=$dbh->prepare(q{
		select id, publicname, email, lang_id, news_read_date
		from users
		where notify_news_email = 1 and id > ? and news_read_date < (select public_date from news_main where id = ?)
		order by id  
	});
	$sth->execute($uid, $ID);
	my $count=0;
	while (my $p = $sth->fetchrow_hashref) {
		my $domain = lc($cfg{'domain'}[$p->{'lang_id'}]);
		my $mailtext = $cfg{'news_email'}[$p->{'lang_id'}];
		$mailtext =~ s|\n|<br/>|ig;
		my $description = qq{
			<a href="https://$domain$cfg{'link'}[$p->{'lang_id'}]" style="color:#4B6999;">$cfg{'header'}[$p->{'lang_id'}]</a><br/>
			$cfg{'description'}[$p->{'lang_id'}]<br/>
		};
		$mailtext =~ s|<!--/name/-->|$p->{'publicname'}|ig;
		$mailtext =~ s|<!--/text/-->|$description|ig;


		my $text = qq{
			<div style="width:100%;background: none repeat scroll 0 0 #4B6999;color:#FFFFFF;font-size:12px;height:20px;padding:6px 0 0px 10px;">$cfg{'ucoin-title'}[$p->{'lang_id'}]</div>
			<br/>
			<table style="width:100%;"><tr>
				<td style="vertical-align:top;">
					<a href="https://$domain$cfg{'link'}[$p->{'lang_id'}]" border="0" title="$cfg{'header'}[$p->{'lang_id'}]"><img src="https://ucoin.net/i/news/$ID.jpg" alt="$cfg{'header'}[$p->{'lang_id'}]" /></a>
				</td>
				<td style="width:20px;"></td>
				<td>$mailtext</td>
			</tr></table>
			<br/>
			<div style="width:100%;background: none repeat scroll 0 0 #4B6999;color:#FFFFFF;font-size:12px;height:21px;padding:5px 0px 0px 10px;">&copy; 2007-2017 uCoin.net
					<div style="float:right;margin-right: 20px;">
						<a href="https://$domain/" style="color:#FFFFFF;">$cfg{'index'}[$p->{'lang_id'}]</a>&#160;&#160;|&#160;&#160;<a href="https://$domain/news/" style="color:#FFFFFF;">$cfg{'news'}[$p->{'lang_id'}]</a>&#160;&#160;|&#160;&#160;<a href="https://$domain/Settings/?v=notifications" style="color:#FFFFFF;">$cfg{'unsubscribe'}[$p->{'lang_id'}]</a>
					</div>
			</div>
		};

		my $pool = `ls -f /var/spool/postfix/active | wc -l`;
		chomp $pool;

		$uptime = `uptime`;
		$uptime =~ m/average: (\d+.\d+)/;
		$la = $1;

		my $mail_prog = '/usr/sbin/sendmail';
		open (MAIL, "|$mail_prog -t");
		print MAIL "Content-type: text/html; charset=UTF-8\n";
		print MAIL "To: <$p->{'email'}>\n";
		print MAIL "From: uCoin.net <noreply\@ucoin.net>\n";
		print MAIL "Subject: $cfg{'header'}[$p->{'lang_id'}]\n";
		print MAIL "\n";
		print MAIL "$text\n";
		close (MAIL);

		$count++;
		my $log = $count . ". UID: " . $p->{'id'} . " <" . $p->{'email'} . "> | LA: $la | POOL: $pool ";


		print IMG "$log\n";
		print $log;

		if ($pool > 50 || $la > 1) {
			if ($la > 7) {sleep 50 ;print "| wait 50s";}
			elsif ($la > 6) {sleep 30 ;print "| wait 30s";}
			elsif ($la > 5) {sleep 20 ;print "| wait 20s";}
			elsif ($la > 4) {sleep 3 ;print "| wait 3s";}
			elsif ($la > 3) {sleep 2 ;print "| wait 2s";}
			elsif ($la > 2) {sleep 1 ;print "| wait 1s";}
			else {sleep 1 ;print "| wait 1s";}
		}
		print "\n";

	}
	close(IMG); 

}

if ($job eq 'subscribe_byr') {

	my %cfg;
	my $query = qq{
		select key, text, lang_id
		from v_translation
		where key in ('domain', 'title', 'index', 'news', 'unsubscribe', 'news_email')
	};
	my $sth=$dbh->prepare($query);
	$sth->execute();
	while (my @row = $sth->fetchrow_array) {
		$cfg{"$row[0]"}[$row[2]] = $row[1];
	}

	$cfg{"header"}[1] = 'Добавлена валюта: Белорусский рубль [BYR]';

	my $sth=$dbh->prepare(q{
		select publicname, email, lang_id
		from users
		where notify_news_email = 1 and country_main_id = 55 and lang_id in (1, 19) 
	});
	$sth->execute();
	my $count=0;
	while (my $p = $sth->fetchrow_hashref) {
		my $domain = lc($cfg{'domain'}[$p->{'lang_id'}]);
		my $description = qq{
			Здравствуйте $p->{'publicname'}, <br/><br/>
			На нашем сайте появилась возможность выбрать белорусский рубль [BYR] в качестве основной валюты для отображения стоимости монет как в каталоге, так и в личной коллекции.<br/><br/>
			Изменить валюту можно на странице <a href="https://$domain/Settings/?v=account" target="_blank">настроек</a>.<br/><br/>
			<br/>
			---<br/>
			С уважением, команда uCoin.net
		};

		my $text = qq{
			<div style="width:100%;background: none repeat scroll 0 0 #4B6999;color:#FFFFFF;font-size:12px;height:20px;padding:6px 0 0px 10px;">uCoin.net - Международный каталог монет мира</div>
			<br/>
			<table style="width:100%;"><tr>
				<td style="vertical-align:top;">
					<a href="https://$domain/Settings/?v=account" border="0"><img src="https://i.ucoin.net/temp/BYR.png" target="_blank"/></a>
				</td>
				<td style="width:20px;"></td>
				<td style="vertical-align: top;font-size:13px;"><br/>$description</td>
				<td style="width:20px;"></td>
			</tr></table>
			<br/>
			<div style="width:100%;background: none repeat scroll 0 0 #4B6999;color:#FFFFFF;font-size:12px;height:21px;padding:5px 0px 0px 10px;">&copy; 2007-2017 uCoin.net
					<div style="float:right;margin-right: 20px;">
						<a href="https://$domain/" style="color:#FFFFFF;">Главная</a>&#160;&#160;|&#160;&#160;<a href="https://$domain/news/" style="color:#FFFFFF;">Новости</a>&#160;&#160;|&#160;&#160;<a href="https://$domain/Settings/?type=notifications" style="color:#FFFFFF;">Отписаться от рассылки</a>
					</div>
			</div>
		};
	
		my $mail_prog = '/usr/sbin/sendmail';
		open (MAIL, "|$mail_prog -t");
		print MAIL "Content-type: text/html; charset=UTF-8\n";
		print MAIL "To: <$p->{'email'}>\n";
		print MAIL "From: uCoin.net <noreply\@ucoin.net>\n";
		print MAIL "Subject: $cfg{'header'}[1]\n";
		print MAIL "\n";
		print MAIL "$text\n";
		close (MAIL);

		$count++;
		print $count . ". " . $p->{'email'} . "\n";
	}

}

############# TEMP ###############
if ($job eq 'temp') {

	my ($uptime, $la);
	my $i = 1;
	my $max = 10000;
	while ($i <= $max ) {


			$uptime = `uptime`;
			$uptime =~ m/average: (\d+.\d+)/;
			$la = $1;
			
			
			my $t1 = `date +%s%N`;

			my $rv = $dbh->do(qq{
				delete from users where id in (
					select id from users where country_main_id = 199 and signup_date > now() - interval '1 day' 
					and id not in (select users_id from session)
					and id not in (select owner_uid from swap_list_log)
					limit 1
				)
			}, undef);
			$dbh->commit();
			$rv = 0 if ($rv eq '0E0');
			my $t2 = `date +%s%N`;
			$t2 = ($t2 - $t1)/1000000000;
			
			
			if ($rv > 0) {
				print "LA: $la | NUMBER: $i | delete users --> $rv [$t2]\n";

#				if ($t2 > 5) {sleep 5;}
#				elsif ($t2 > 4) {sleep 4;}
#				elsif ($t2 > 3) {sleep 3;}
#				elsif ($t2 > 2) {sleep 2;}
#				elsif ($t2 > 1) {sleep 1;}
#				else {sleep 0.5;}

				sleep $la/2;
			}
#		}
#		if ($la < 3) {$i+=2000;}
#		elsif ($la < 4) {$i+=1000;}
#		else {$i+=500;}
		$i++;
	}

}



$dbh->disconnect;

sub url_encode {
    my $string = shift;
  
    $string =~ s/%3F/\?/g;
	$string =~ s/%3D/\=/g;
  
    return $string;
}