#!/usr/bin/perl

use strict;
use Getopt::Long;

my $freq_file = ();
my $stop_words_file = ();

# processing command line arguments
GetOptions (
              "f=s"   => \$freq_file,   
              "s=s"   => \$stop_words_file )  or die("Usage: $0 -f <frequency_file_name> -s <stop_words_file_name>\n");

if( !defined($freq_file) || !defined($stop_words_file) ){
	die("Usage: $0 -f <frequency_file_name> -s <stop_words_file_name>\n");
}

# variable for the final list of words and counts
my %FINAL_LIST = ();

# read the stop words file in an array in memory
open(FILE, "<$stop_words_file") or die "Cannot find stopwords file";
my @sw = <FILE>;
close(FILE);

# copy the stop words array into a hash	
my %STOP_WORDS = map { chomp; $_ => 1 } @sw;

# read the results file copied from hadoop into memory for processing
open(FILE, "<$freq_file") or die "Cannot open the data file";
my @words = <FILE>;
close(FILE);


foreach my $word_line (@words){
	my @tokens = split(/\s/, $word_line);	
	my $word = $tokens[0];
	
	# strip out all non-letters to better compare with the stop words file
	$word =~ s/[^a-zA-Z]//g;

	# ignore stop words and single letters
	next if (length($word) < 2 || exists($STOP_WORDS{lc($word)}));

	# store the initial word and its frequency in the final list
	$FINAL_LIST{$tokens[0]} = $tokens[1];
}

# print the final list in the reverse order of their frequency
foreach my $v ( sort { $FINAL_LIST{$b} <=> $FINAL_LIST{$a} } keys %FINAL_LIST){
	print $v . " ". $FINAL_LIST{$v}. " \n";
}


