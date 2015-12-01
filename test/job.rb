#!  /usr/bin/ruby

if ARGV.empty?
  puts "Usage: #{$0} <seed>"
  exit 1
end

taskid = ARGV.first.to_i

if taskid == 0
  puts "Invalid taskid #{ARGV[0]}"
  exit 1
end

COUNT=10
TAU=5.0
SEED=2048 + 3*taskid

if Object.const_defined? :Random
  $rng = Random.new SEED
  def rng
    $rng.rand
  end
else
  def rng
    rand
  end
end

total = 0.0
for i in (1..COUNT)
  secs = -TAU*Math::log( rng() )
  # secs = -Math::log(rng.rand)
  STDOUT.puts "Waiting #{secs} seconds"
  STDOUT.flush
  sleep(secs)
  total += secs
end

puts "Waited #{total} secs"

