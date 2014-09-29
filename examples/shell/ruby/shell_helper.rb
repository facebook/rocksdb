require 'ruby/db_wrapper'

require 'readline'
require 'securerandom'

module Rocksby
  class ShellHelper
    def initialize(db_name = SecureRandom.hex)
      @name    = db_name
      @db      = DBWrapper.new(@name)
      @history = []
    end

    def loop
      input = Readline::readline("#rocksdb[#{@name}]$ ", true)
      unless input
        puts
        exit
      end

      if input =~ /^\s*$/ ||
        (Readline::HISTORY.size > 1 && input == Readline::HISTORY[-2])
        Readline::HISTORY.pop
      end

      input = input.chomp.split

      return if input.empty?

      case input.first
      when 'help'
        shell_help
      when 'get'
        shell_get(input[1])
      when 'put'
        shell_put(input[1], input[2])
      when 'quit'
        exit
      when ''
      else
        puts 'error: not a command; type "help" to view commands'
      end
    end

    private

    def shell_help
      puts 'commands list'
      puts '  * get [key]'
      puts '      -- Prints out value for given key'
      puts '  * put [key] [value]'
      puts '      -- Stores value for given key'
      puts '  * help'
      puts '      -- Prints out this help text'
    end

    def shell_get(key)
      unless key
        puts 'error: no key specified'
        return
      end

      value = @db.get(key)
      puts value ? "\"#{value}\"" : "no value for key \"#{key}\""
    end

    def shell_put(key, value)
      unless key
        puts 'error: no key or value specified'
        return
      end

      unless value
        puts 'error: no value specified'
        return
      end

      return if @db.put(key, value)

      puts "error: unable to put \"#{key}\" \"#{value}\" into database"
    end
  end
end
