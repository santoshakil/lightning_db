import 'package:flutter/material.dart';
import 'package:lightning_db/lightning_db.dart';
import '../models/user_model.dart';
import '../models/post_model.dart';

class QueryBuilderScreen extends StatefulWidget {
  const QueryBuilderScreen({super.key});

  @override
  State<QueryBuilderScreen> createState() => _QueryBuilderScreenState();
}

class _QueryBuilderScreenState extends State<QueryBuilderScreen> {
  late LightningDb _db;
  late FreezedCollection<User> _users;
  late FreezedCollection<Post> _posts;
  
  List<dynamic> _results = [];
  String _queryDescription = '';
  String _queryPlan = '';
  bool _isLoading = false;

  @override
  void initState() {
    super.initState();
    _initDatabase();
  }

  Future<void> _initDatabase() async {
    _db = await LightningDb.open('query_builder_demo.db');
    _users = _db.freezedCollection<User>('users');
    _posts = _db.freezedCollection<Post>('posts');
    
    // Seed test data
    await _seedTestData();
  }

  Future<void> _seedTestData() async {
    // Check if already seeded
    if (await _users.query().exists()) return;
    
    // Create test users
    final users = [
      User(
        id: 'user1',
        name: 'Alice Johnson',
        email: 'alice@example.com',
        age: 28,
        createdAt: DateTime.now().subtract(const Duration(days: 30)),
        metadata: {
          'city': 'New York',
          'country': 'USA',
          'active': true,
          'premium': true,
          'score': 95.5,
          'tags': ['developer', 'flutter', 'mobile'],
        },
      ),
      User(
        id: 'user2',
        name: 'Bob Smith',
        email: 'bob@example.com',
        age: 35,
        createdAt: DateTime.now().subtract(const Duration(days: 20)),
        metadata: {
          'city': 'San Francisco',
          'country': 'USA',
          'active': true,
          'premium': false,
          'score': 78.2,
          'tags': ['designer', 'ui', 'web'],
        },
      ),
      User(
        id: 'user3',
        name: 'Charlie Brown',
        email: 'charlie@example.com',
        age: 42,
        createdAt: DateTime.now().subtract(const Duration(days: 10)),
        metadata: {
          'city': 'London',
          'country': 'UK',
          'active': false,
          'premium': true,
          'score': 88.7,
          'tags': ['manager', 'agile'],
        },
      ),
      User(
        id: 'user4',
        name: 'Diana Prince',
        email: 'diana@example.com',
        age: 25,
        createdAt: DateTime.now().subtract(const Duration(days: 5)),
        metadata: {
          'city': 'Paris',
          'country': 'France',
          'active': true,
          'premium': true,
          'score': 92.1,
          'tags': ['developer', 'backend', 'cloud'],
        },
      ),
      User(
        id: 'user5',
        name: 'Eve Wilson',
        email: 'eve@example.com',
        age: 30,
        createdAt: DateTime.now().subtract(const Duration(days: 1)),
        metadata: {
          'city': 'Tokyo',
          'country': 'Japan',
          'active': true,
          'premium': false,
          'score': 81.3,
          'tags': ['developer', 'mobile', 'ios'],
        },
      ),
    ];
    
    await _users.addAll(users);
    
    // Create test posts
    final posts = [
      Post(
        id: 'post1',
        title: 'Getting Started with Flutter',
        content: 'Flutter is an amazing framework for building beautiful apps...',
        authorId: 'user1',
        createdAt: DateTime.now().subtract(const Duration(hours: 24)),
        tags: ['flutter', 'tutorial', 'mobile'],
        metadata: {
          'views': 1250,
          'likes': 85,
          'published': true,
        },
      ),
      Post(
        id: 'post2',
        title: 'Advanced State Management',
        content: 'State management is crucial for large Flutter applications...',
        authorId: 'user1',
        createdAt: DateTime.now().subtract(const Duration(hours: 12)),
        tags: ['flutter', 'state', 'advanced'],
        metadata: {
          'views': 890,
          'likes': 62,
          'published': true,
        },
      ),
      Post(
        id: 'post3',
        title: 'UI Design Principles',
        content: 'Good design is essential for user experience...',
        authorId: 'user2',
        createdAt: DateTime.now().subtract(const Duration(hours: 6)),
        tags: ['design', 'ui', 'principles'],
        metadata: {
          'views': 543,
          'likes': 31,
          'published': false,
        },
      ),
    ];
    
    await _posts.addAll(posts);
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Query Builder Demo'),
      ),
      body: Row(
        children: [
          // Query options panel
          Container(
            width: 350,
            decoration: BoxDecoration(
              color: Colors.grey[100],
              border: Border(
                right: BorderSide(color: Colors.grey[300]!),
              ),
            ),
            child: ListView(
              padding: const EdgeInsets.all(16),
              children: [
                _buildSection('Basic Queries', [
                  _buildQueryButton(
                    'All Users',
                    () => _executeQuery(
                      _users.query(),
                      'Get all users',
                    ),
                  ),
                  _buildQueryButton(
                    'Active Users',
                    () => _executeQuery(
                      _users.query().where('metadata.active', isEqualTo: true),
                      'Find active users',
                    ),
                  ),
                  _buildQueryButton(
                    'Premium Users',
                    () => _executeQuery(
                      _users.query().where('metadata.premium', isEqualTo: true),
                      'Find premium users',
                    ),
                  ),
                ]),
                _buildSection('Comparison Queries', [
                  _buildQueryButton(
                    'Users Over 30',
                    () => _executeQuery(
                      _users.query().where('age', isGreaterThan: 30),
                      'Find users older than 30',
                    ),
                  ),
                  _buildQueryButton(
                    'High Score Users',
                    () => _executeQuery(
                      _users.query().where('metadata.score', isGreaterThanOrEqualTo: 90),
                      'Find users with score >= 90',
                    ),
                  ),
                  _buildQueryButton(
                    'Recent Users',
                    () => _executeQuery(
                      _users.query().createdAfter(
                        DateTime.now().subtract(const Duration(days: 15)),
                      ),
                      'Find users created in last 15 days',
                    ),
                  ),
                ]),
                _buildSection('Text Search', [
                  _buildQueryButton(
                    'Search by Name',
                    () => _executeQuery(
                      _users.query().where('name', contains: 'e'),
                      'Find users with "e" in name',
                    ),
                  ),
                  _buildQueryButton(
                    'Email Domain',
                    () => _executeQuery(
                      _users.query().where('email', endsWith: '@example.com'),
                      'Find users with @example.com email',
                    ),
                  ),
                  _buildQueryButton(
                    'Multi-field Search',
                    () => _executeQuery(
                      _users.query().textSearch(['name', 'email'], 'alice').search(),
                      'Search "alice" in name or email',
                    ),
                  ),
                ]),
                _buildSection('Array Queries', [
                  _buildQueryButton(
                    'Developer Tag',
                    () => _executeQuery(
                      _users.query().where('metadata.tags', contains: 'developer'),
                      'Find users with developer tag',
                    ),
                  ),
                  _buildQueryButton(
                    'Multiple Tags',
                    () => _executeQuery(
                      _users.query().where(
                        'metadata.tags',
                        containsAny: ['flutter', 'mobile'],
                      ),
                      'Find users with flutter OR mobile tags',
                    ),
                  ),
                  _buildQueryButton(
                    'All Tags',
                    () => _executeQuery(
                      _users.query().where(
                        'metadata.tags',
                        containsAll: ['developer', 'mobile'],
                      ),
                      'Find users with developer AND mobile tags',
                    ),
                  ),
                ]),
                _buildSection('Complex Queries', [
                  _buildQueryButton(
                    'Active Premium Developers',
                    () => _executeQuery(
                      _users.query()
                        .where('metadata.active', isEqualTo: true)
                        .where('metadata.premium', isEqualTo: true)
                        .where('metadata.tags', contains: 'developer'),
                      'Active + Premium + Developer',
                    ),
                  ),
                  _buildQueryButton(
                    'US or UK Users',
                    () => _executeQuery(
                      _users.query().where(
                        'metadata.country',
                        whereIn: ['USA', 'UK'],
                      ),
                      'Find users from USA or UK',
                    ),
                  ),
                  _buildQueryButton(
                    'Not from US',
                    () => _executeQuery(
                      _users.query().where(
                        'metadata.country',
                        whereNotIn: ['USA'],
                      ),
                      'Find non-US users',
                    ),
                  ),
                ]),
                _buildSection('Sorting & Pagination', [
                  _buildQueryButton(
                    'Sort by Age',
                    () => _executeQuery(
                      _users.query().orderBy('age'),
                      'Sort users by age (ascending)',
                    ),
                  ),
                  _buildQueryButton(
                    'Sort by Score (Desc)',
                    () => _executeQuery(
                      _users.query().orderBy('metadata.score', descending: true),
                      'Sort by score (descending)',
                    ),
                  ),
                  _buildQueryButton(
                    'Paginated Results',
                    () => _executePaginatedQuery(
                      _users.query().orderBy('createdAt', descending: true),
                      'Get page 1 (2 items per page)',
                    ),
                  ),
                ]),
                _buildSection('Aggregations', [
                  _buildQueryButton(
                    'User Statistics',
                    () => _executeAggregation(
                      _users.query()
                        .aggregate()
                        .count('total')
                        .average('age', 'avgAge')
                        .min('age', 'minAge')
                        .max('age', 'maxAge')
                        .average('metadata.score', 'avgScore'),
                      'Calculate user statistics',
                    ),
                  ),
                  _buildQueryButton(
                    'Premium Stats',
                    () => _executeAggregation(
                      _users.query()
                        .where('metadata.premium', isEqualTo: true)
                        .aggregate()
                        .count('premiumUsers')
                        .average('metadata.score', 'avgPremiumScore'),
                      'Premium user statistics',
                    ),
                  ),
                ]),
                _buildSection('Cross-Collection', [
                  _buildQueryButton(
                    'Posts by Author',
                    () => _executeQuery(
                      _posts.query().where('authorId', isEqualTo: 'user1'),
                      'Find posts by user1',
                    ),
                  ),
                  _buildQueryButton(
                    'Popular Posts',
                    () => _executeQuery(
                      _posts.query()
                        .where('metadata.views', isGreaterThan: 800)
                        .orderBy('metadata.likes', descending: true),
                      'Posts with >800 views, sorted by likes',
                    ),
                  ),
                ]),
              ],
            ),
          ),
          // Results panel
          Expanded(
            child: Column(
              children: [
                // Query description
                if (_queryDescription.isNotEmpty)
                  Container(
                    width: double.infinity,
                    padding: const EdgeInsets.all(16),
                    color: Colors.blue[50],
                    child: Column(
                      crossAxisAlignment: CrossAxisAlignment.start,
                      children: [
                        Text(
                          'Query: $_queryDescription',
                          style: const TextStyle(
                            fontWeight: FontWeight.bold,
                            fontSize: 16,
                          ),
                        ),
                        if (_queryPlan.isNotEmpty) ...[
                          const SizedBox(height: 8),
                          Text(
                            _queryPlan,
                            style: TextStyle(
                              fontFamily: 'monospace',
                              fontSize: 12,
                              color: Colors.grey[700],
                            ),
                          ),
                        ],
                      ],
                    ),
                  ),
                // Results
                Expanded(
                  child: _isLoading
                      ? const Center(child: CircularProgressIndicator())
                      : _results.isEmpty
                          ? const Center(
                              child: Text(
                                'Select a query to see results',
                                style: TextStyle(
                                  fontSize: 18,
                                  color: Colors.grey,
                                ),
                              ),
                            )
                          : ListView.builder(
                              padding: const EdgeInsets.all(16),
                              itemCount: _results.length,
                              itemBuilder: (context, index) {
                                final item = _results[index];
                                return Card(
                                  margin: const EdgeInsets.only(bottom: 8),
                                  child: Padding(
                                    padding: const EdgeInsets.all(16),
                                    child: _buildResultItem(item),
                                  ),
                                );
                              },
                            ),
                ),
                // Results count
                if (_results.isNotEmpty)
                  Container(
                    width: double.infinity,
                    padding: const EdgeInsets.all(16),
                    color: Colors.grey[200],
                    child: Text(
                      'Results: ${_results.length} items',
                      style: const TextStyle(fontWeight: FontWeight.bold),
                    ),
                  ),
              ],
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildSection(String title, List<Widget> children) {
    return ExpansionTile(
      title: Text(
        title,
        style: const TextStyle(fontWeight: FontWeight.bold),
      ),
      initiallyExpanded: true,
      children: children,
    );
  }

  Widget _buildQueryButton(String label, VoidCallback onPressed) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 4),
      child: ElevatedButton(
        onPressed: onPressed,
        style: ElevatedButton.styleFrom(
          alignment: Alignment.centerLeft,
        ),
        child: Text(label),
      ),
    );
  }

  Widget _buildResultItem(dynamic item) {
    if (item is User) {
      return Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            item.name,
            style: const TextStyle(
              fontWeight: FontWeight.bold,
              fontSize: 16,
            ),
          ),
          const SizedBox(height: 4),
          Text('Email: ${item.email}'),
          Text('Age: ${item.age ?? 'N/A'}'),
          if (item.metadata != null) ...[
            const SizedBox(height: 8),
            Text(
              'Metadata:',
              style: TextStyle(
                fontWeight: FontWeight.bold,
                color: Colors.grey[600],
              ),
            ),
            ...item.metadata!.entries.map((e) => Text('  ${e.key}: ${e.value}')),
          ],
        ],
      );
    } else if (item is Post) {
      return Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          Text(
            item.title,
            style: const TextStyle(
              fontWeight: FontWeight.bold,
              fontSize: 16,
            ),
          ),
          const SizedBox(height: 4),
          Text(
            item.content,
            maxLines: 2,
            overflow: TextOverflow.ellipsis,
          ),
          const SizedBox(height: 4),
          Text('Author: ${item.authorId}'),
          Text('Tags: ${item.tags.join(', ')}'),
          if (item.metadata != null) ...[
            const SizedBox(height: 4),
            Text('Views: ${item.metadata!['views']} | Likes: ${item.metadata!['likes']}'),
          ],
        ],
      );
    } else if (item is Map) {
      // Aggregation results
      return Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: item.entries.map((e) => Text(
          '${e.key}: ${e.value is double ? e.value.toStringAsFixed(2) : e.value}',
          style: const TextStyle(fontSize: 14),
        )).toList(),
      );
    } else {
      return Text(item.toString());
    }
  }

  Future<void> _executeQuery<T>(
    CollectionQuery<T> query,
    String description,
  ) async {
    setState(() {
      _isLoading = true;
      _queryDescription = description;
      _queryPlan = '';
    });

    try {
      // Enable explain for advanced queries
      if (query is CollectionQuery) {
        final builder = (query as dynamic)._builder as AdvancedQueryBuilder;
        builder.explain();
      }

      final results = await query.findAll();
      
      setState(() {
        _results = results;
        _isLoading = false;
      });
    } catch (e) {
      setState(() {
        _results = [];
        _isLoading = false;
      });
      
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Query error: $e'),
          backgroundColor: Colors.red,
        ),
      );
    }
  }

  Future<void> _executePaginatedQuery<T>(
    CollectionQuery<T> query,
    String description,
  ) async {
    setState(() {
      _isLoading = true;
      _queryDescription = description;
    });

    try {
      final pagedResult = await query.page(1, pageSize: 2);
      
      setState(() {
        _results = pagedResult.items;
        _queryDescription = '$description\nPage ${pagedResult.page}/${pagedResult.totalPages} '
            '(${pagedResult.totalItems} total items)';
        _isLoading = false;
      });
    } catch (e) {
      setState(() {
        _results = [];
        _isLoading = false;
      });
      
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Query error: $e'),
          backgroundColor: Colors.red,
        ),
      );
    }
  }

  Future<void> _executeAggregation(
    AggregationQuery query,
    String description,
  ) async {
    setState(() {
      _isLoading = true;
      _queryDescription = description;
    });

    try {
      final results = await query.execute();
      
      setState(() {
        _results = [results];
        _isLoading = false;
      });
    } catch (e) {
      setState(() {
        _results = [];
        _isLoading = false;
      });
      
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Aggregation error: $e'),
          backgroundColor: Colors.red,
        ),
      );
    }
  }

  @override
  void dispose() {
    _db.close();
    super.dispose();
  }
}