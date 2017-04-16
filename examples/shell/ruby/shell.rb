require 'ruby/shell_helper'

shell_helper = Rocksby::ShellHelper.new

trap("SIGINT") do
  puts
  exit!
end

loop { shell_helper.loop }
