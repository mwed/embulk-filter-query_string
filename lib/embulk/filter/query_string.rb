Embulk::JavaPlugin.register_filter(
  "query_string", "org.embulk.filter.query_string.QueryStringFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
