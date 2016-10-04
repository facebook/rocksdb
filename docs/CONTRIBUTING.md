This provides guidance on how to contribute various content to `rocksdb.org`.

## Landing page

Modify `index.md` with your new or updated content.

If you want a `GridBlock` as part of your content, you can do so directly with HTML:

```
<div class="gridBlock">
  <div class="blockElement twoByGridBlock alignLeft">
    <div class="blockContent">
      <h3>HHVM Features</h3>
      <ul>
        <li>The <a href="http://example.org/">Example</a></li>
        <li><a href="http://example.com">Another Example</a></li>
      </ul>
    </div>
  </div>

  <div class="blockElement twoByGridBlock alignLeft">
    <div class="blockContent">
      <h3>More information</h3>
      <p>
         Stuff here
      </p>
    </div>
  </div>
</div>
```

or with a combination of changing `./_data/features.yml` and adding some Liquid to `index.md`, such as:

```
{% include content/gridblocks.html data_source=site.data.features imagealign="bottom"%}
```

## Blog

Adding a new blog post is a four-step process.

> Some posts have a `permalink` and `comments` in the blog post YAML header. You will not need these for new blog posts. These are an artifact of migrating the blog from Wordpress to gh-pages.

1. Create your blog post in `./_posts` in markdown (file extension `.md` or `.markdown`). See current posts in that folder or `2016-04-07-blog-post-example.md` for an example of the YAML format.
  - You can add a `<!--truncate-->` tag in the middle of your post such that you show only the excerpt above that tag in the main `/blog` index on your page.
1. If you have not authored a blog post before, modify the `./_data/authors.yml` file with the `author` id you used in your blog post, along with your full name and Facebook ID to get your profile picture.
1. [Run the site locally](./README.md) to test your changes. It will be at `http://127.0.0.1/blog/your-new-blog-post-title.html`
1. Push your changes to GitHub.

## Docs

To modify docs, edit the appropriate markdown file in `./_docs`.

To add docs to the site, ....

1. Add your markdown file to the `./_docs` folder. See `./docs-hello-world.md` for an example of the YAML header format.
  - You can use folders in the `./_docs` directory to organize your content if you want.
1. Update `_data/nav_docs.yml` to add your new document to the navigation bar. Use the `id` you put in your doc markdown in ad the id in the `_data/nav_docs.yml` file.
1. [Run the site locally](./README.md) to test your changes. It will be at `http://127.0.0.1/docs/your-new-doc-permalink.html`
1. Push your changes to GitHub.

## Header Bar

To modify the header bar, change `./_data/nav.yml`.

## Other Changes

- CSS: `./css/main.css` or `./_sass/*.scss`.
- Images: `./static/images/[docs | posts]/....`
- Main Blog post HTML: `./_includes/post.html`
- Main Docs HTML: `./_includes/doc.html`
