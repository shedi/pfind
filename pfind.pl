#!/usr/bin/perl

#Copyright 2013 Edi Shmueli
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

#--------------------------------------
#Parallel (multi-threaded) find utility 
#--------------------------------------

use strict;
use warnings;
use Getopt::Long;
use Switch;
use threads;
use Thread::Queue;
use IO::Dir;
use IO::Handle;
use File::stat;
use List::Util qw(sum);
use Time::HiRes qw(gettimeofday tv_interval);;

####################################
#Globals
####################################
my $verbose='';
my $filter='';

#-----------------------------------
#Statistics
#-----------------------------------
my $dirs_scanned :shared;
my $files_scanned :shared;
my (@queue_pending_vec,@dirs_diff_vec,@files_diff_vec) :shared;

#-----------------------------------
#Thread control
#-----------------------------------
my @thread_list = ();
my $monitor_thr;
my $block_queue = Thread::Queue->new;
my $work_queue = Thread::Queue->new;
my $active :shared;
my $continue_monitor :shared;

####################################
#Worker
####################################
sub worker {
  my $tid = threads->self->tid;

  while (my $dir=$work_queue->dequeue()) {
    {
      lock($dirs_scanned);
      $dirs_scanned++;
    }

    if ($filter ne "files") {
      print $dir."\n";
    }

    #my $t0 = [gettimeofday];
    if ( (-e $dir) && (! -l $dir) ) {
      if ( ! -x $dir) {
	print STDERR $0.": ".$dir.": Permission denied\n";
      } else {
	my(@list, $d);
	$d = IO::Dir->new($dir);
	if (defined($d)) {

	  @list = $d->read();

	  @list = grep !/^\.{1,2}$/, @list;
	
	  my (@dirs_found,@files_found);

	  for (@list) {
	    my $fullname=$dir."/".$_;
	    my $s1=stat($fullname);
	    if ( defined $s1  ){
	      if ( -l $fullname ){
		if ( $filter eq '' ){
		  print $fullname."\n";
		}
	      }else{
		 if ($s1->mode & 0040000) {
		   push(@dirs_found,$fullname);
		 }else{
		   push(@files_found,$fullname);
		   if ($filter ne "dirs") {
		    print $fullname."\n";
		  }
		 }
	       }
	    }else{ #broken symlink
	      if ( $filter eq ''){
		print $fullname."\n";
	      }
	    }
	  }
	  {
	    lock($active);
	    $active+=scalar @dirs_found;
	  }
	  {
	    lock($files_scanned);
	    $files_scanned+=scalar @files_found;
	  }

	  $work_queue->enqueue(@dirs_found);
	}
      }
      #my $elapsed = tv_interval ($t0);
      #print "elapsed=".$elapsed."\n";	  
    }#if (-e $dir)...
    {
      lock($active);
      $active--;
      if ($active == 0) {
	$block_queue->enqueue(undef);
      }
    }

  }#while
}

####################################
#Monitor
####################################
sub monitor_thread {
  my $tid = threads->self->tid;
  my $last_files_scanned=0;
  my $last_dirs_scanned=0;
  my ($pending,$dirs_diff,$files_diff);
  my $continue=1;

  local $SIG{KILL} = sub {threads->exit;};

  while ($continue) {
    {
      lock($dirs_scanned);
      $dirs_diff=$dirs_scanned-$last_dirs_scanned;
      $last_dirs_scanned=$dirs_scanned;
    }
    push(@dirs_diff_vec,$dirs_diff);

    {
      lock($files_scanned);
      $files_diff=$files_scanned-$last_files_scanned;
      $last_files_scanned=$files_scanned;
    }
    push(@files_diff_vec,$files_diff);

    $pending=$work_queue->pending();
    push(@queue_pending_vec,$pending);


    print STDERR sprintf("%10d%10d%10d\n",$pending,$dirs_diff,$files_diff);
    {
      lock($continue_monitor);
      if ($continue_monitor==0) {
	$continue=0;
      }
    }
    sleep(1);
  }
}

sub HelpMessag{
  print STDERR "Usage: pfind.pl [path] [-type <f|d>] [-num_threads <n>] [-monitor] [-verbose] [-help|-h]\n";
  print STDERR "Parallel (multi-threaded) \'find\' utility Written by Edi Shmueli, 2013\n";
  exit 0;
}

####################################
#Main
####################################
{
  #-----------------------------------
  # Parse Args
  #-----------------------------------
  my $start_dir='.';
  my $arg_type='';
  my $num_threads = 8;
  my $monitor='';

  GetOptions ('help|h' => sub{HelpMessag()},'num_threads=i' => \$num_threads, 'type=s' => \$arg_type,'verbose' => \$verbose,'monitor' => \$monitor);
  switch ($arg_type){
    case "f" {$filter="files"}
      case "d" {$filter="dirs"}
	case "" {}
	  else {
	    print "find: invalid argument `".$arg_type."' to `-type option\n"; exit 1;
	  }
    ;
  }

  if (defined $ARGV[0]) {
    $start_dir = $ARGV[0];
  }

  if ($verbose) {
    print STDERR "Start dir = ".$start_dir."\n";
    print STDERR "Num threads = ".$num_threads."\n";
    print STDERR "Filter=".$filter."\n";
  }


  #-----------------------------------
  #initiate work
  #-----------------------------------
  {
    lock($active);
    $active=1;
  }
  {
    lock($files_scanned);
    $files_scanned=0;
  }
  {
    lock($dirs_scanned);
    $dirs_scanned=0;
  }
  foreach (1..$num_threads) {
    my $thr = threads->new(\&worker);
    push(@thread_list, $thr);
  }


  if ($monitor) {
    {
      lock $continue_monitor;
      $continue_monitor=1;
    }
    print STDERR sprintf("%10s%10s%10s\n","Queue","Dirs/s","Files/s");
    $monitor_thr = threads->new(\&monitor_thread);
  }

  $work_queue->enqueue($start_dir);
  $block_queue->dequeue();

  foreach (1..$num_threads) {
    $work_queue->enqueue(undef);
  }


  for (@thread_list) {
    $_->join();
  }

  if ($monitor) {
    $monitor_thr->kill(9)->join();
    print STDERR sprintf("%10s%10s%10s\n","----------","----------","----------");
    print STDERR sprintf("%10s%10d%10d\n","Average",sum(@dirs_diff_vec)/@dirs_diff_vec,sum(@files_diff_vec)/@files_diff_vec);
  }
}
