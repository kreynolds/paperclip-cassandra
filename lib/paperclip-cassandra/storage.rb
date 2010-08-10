module Paperclip
	module Storage
		module Cassandra
			def self.extended base
				base.instance_eval do
					@column_family = @options[:column_family] || raise("A column family must be specified")
					@connection = @options[:connection] || raise("A connection must be specified")
					@key_prefix = @options[:key_prefix] || @instance.class.to_s
				end
			end

			# Return a key for this particular Attachment
			def key
				"#{@key_prefix}/#{@instance.to_param}"
			end

			def exists?(style_name = default_style)
				if original_filename
					file = @connection.get(@column_family, key, style_name.to_s)
					return file if !file.nil?
				else
					false
				end
			end

			# Returns representation of the data of the file assigned to the given
			# style, in the format most representative of the current storage.
			def to_file style_name = default_style
				return @queued_for_write[style_name] if @queued_for_write[style_name]
				
				if file = exists?(style_name)
					return StringIO.new(file['content'])
				end
				
				nil
			end

			def flush_writes #:nodoc:
				@queued_for_write.each do |style_name, file|
					file.seek(0)
					@connection.insert(@column_family, key, { style_name.to_s => {'created_at' => Time.new, 'content' => file.read, 'size' => file.size, 'content_type' => @instance.image_content_type} })
				end
				@queued_for_write = {}
			end

			def flush_deletes #:nodoc:
				@queued_for_delete.each do |style_name|
					@connection.remove(@column_family, key, style_name)
				end
				@queued_for_delete = []
			end
		end
	end
end
