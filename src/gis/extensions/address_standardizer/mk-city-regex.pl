#!  /usr/bin/perl
#!/usr/bin/perl -w
use strict;
use Regexp::Assemble;

my @cities = split(/[\r\n]+/, qx(cat usps-st-city-name.txt));

my %st= ();
for my $x (@cities) {
    my ($st, $ct) = split(/\t/, $x);
    push @{$st{$st}}, $ct;
}

my $re;
my $ra = Regexp::Assemble->new(flags => "i");

my %re =();
for my $x (sort keys %st) {
    $ra->add(@{$st{$x}});
    $re = $ra->re;
    $re =~ s/\\/\\\\/g;
    $re{$x} = $re;
}

print "#define NUM_STATES " . scalar (keys %re) . "\n\n";
print "    static const char *states[NUM_STATES] = \n";
print "        {\"" . join('","', sort keys %re) . "\"};\n\n";
print "    static const char *stcities[NUM_STATES] = {\n";
my $cnt = 0;
my $a = '';
my $b = '';
for my $x (sort keys %re) {
    $re = "(?:\\\\b)($re{$x})\$";
    print "  ,\n" if $cnt; 
    print "  /* -- $x -- $x -- $x -- $x -- $x -- $x -- $x -- $x -- $x -- $x -- */\n";
    while ($re =~ s/^(.{1,65})//) {
        $a = $1;
        if ($a =~ s/(\\+)$//) {
            print "  \"$b$a\"\n";
            $b = $1;
        }
        else {
            print "  \"$b$a\"\n";
            $b = '';
        }
    }
    $cnt++;
}
print "    };\n";

