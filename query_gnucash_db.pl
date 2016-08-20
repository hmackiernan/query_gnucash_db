use DBI;
use DBD::mysql;
use Data::Dumper;
use strict;
use Getopt::Long;
my ($start_dt, $end_dt);

my %opt = ('start_dt' => '2016-01-01',
	   'end_dt' => '2016-09-01',
	   );
	   
GetOptions(\%opt,"start_dt:s","end_dt:s");

print Dumper(\%opt);

my $query=<<'QUERY';
select cast(sum(amount)  as decimal(8,2)) as 'Total  amount',
    concat(coalesce(grandparent_name, ''),
        if(grandparent_name is null, '', ' > '),
        coalesce(parent_name, ''),
        if(parent_name is null, '', ' > '),
        name) as name
from (
    select date_format(posted, '%Y-%m') as month,
        a.name,
        aa.name as parent_name,
        aaa.name as grandparent_name,
        sum(amount) as amount
    from transaction as t
        inner join split as s on s.transaction = t.id
        inner join (
            select id, name, parent from account
            where type='EXPENSE'
        ) as a on a.id = s.account
        left outer join account as aa on aa.id = a.parent
        left outer join account as aaa on aaa.id = aa.parent
    where posted >= ?
    and posted <= ?
    and aa.name like "Book%"
    group by date_format(posted, '%Y-%m'), a.name
) as x
group by name
order by name;
QUERY
;




my $dsn = 'DBI:mysql:gnucash_db:localhost';
my $db_user_name='root';
my $db_password='root';

my $dbh = DBI->connect($dsn,$db_user_name,$db_password) or die "Failed:";
my $sth = $dbh->prepare($query) or die "Failed: $dbh->errstr";

# For some reason, we have to explicitly call bind param, executing the statement
# using the hash value returns nothing, while passing them in as variables works.
# The explicit bind works with the hash. Go figure.

$sth->bind_param(1,$opt{'start_dt'});
$sth->bind_param(2,$opt{'end_dt'});
$sth->execute();



my $total_hr;
while (my $hash_ref = $sth->fetchrow_hashref) {
  $total_hr->{$hash_ref->{"name"}} = $hash_ref->{"Total  amount"};
}
print Dumper($total_hr);

