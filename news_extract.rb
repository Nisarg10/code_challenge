require 'rubygems'
require 'redis'
require 'nokogiri'
require 'httparty'
require 'pry'
require 'zip'
require 'json'
require 'open-uri'

class News_extract
  def catch_zip_url(url)
   # url = 'http://bitly.com/nuvi-plz'
    page = HTTParty.get(url)
    parsed_page = Nokogiri::HTML(page)
    zip_urls = []
    Zip.warn_invalid_date = false
    parsed_page.css('tr').css('td').css('a').map do |p|
      if p['href'].include?('zip')
        zip_urls.push(url + p['href'])
      end
    end
    #puts zip_urls
    open_zip(zip_urls)
    #Pry.start(binding)
    puts 'Finish..!'
  end

  def open_zip(zip_urls)
    redis = Redis.new
    uniqe_reports = []
    zip_urls.each do |zip|
      file = HTTParty.get(zip).body
      Zip::File.open_buffer(file) do |xml_files|
        xml_files.each do |entry|
          #file_name = entry.name
            file_content = entry.get_input_stream.read
            report_url = Nokogiri::XML(file_content).xpath("//topic_url")[0].to_s

          while !uniqe_reports.include? (report_url)
            uniqe_reports.push(report_url)
            puts 'Loading Data...!'
            redis.rpush 'NEWS_XML', file_content
            redis.publish 'NEWS_XML', file_content
          end
          end
        end
      end
    end
  end



News_extract.new.catch_zip_url('http://feed.omgili.com/5Rh5AMTrc4Pv/mainstream/posts/')