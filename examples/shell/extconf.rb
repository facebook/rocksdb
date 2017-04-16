require 'mkmf'
dir_config('rocksdb')
$CPPFLAGS += ' -std=c++11'
create_makefile('rocksby', 'src')

lines = File.readlines('Makefile')
lines.each do |line|
  next unless line.start_with?('LDSHAREDXX = $(CXX) -shared')
  line.chomp!
  line << " librocksdb.so\n"
end

File.delete('Makefile')
File.open('Makefile', 'w') { |f| f.puts(lines) }
