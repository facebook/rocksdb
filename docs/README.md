# Facebook Open Source Project Site Template: Jekyll Edition

This is a template for use with Jekyll. You can use it directly on a `gh-pages` branch where it will automatically serve up the content, or you can put it in your `master` branch using a script to copy the static generated markup (from the `_site` folder) into the gh-pages branch.

## Getting Started

Clone the contents of this folder, install Jekyll (currently targeting version 3.0), and then run:

```
jekyll serve --config=_config.yml,_config_local_dev.yml
```

This will serve up the site on your local device at http://127.0.0.1:4000/ - the `_config_local_dev` file over-rides some URL settings that you might be using in production to allow you to test locally without pesky relative URLs.

## Setting it Up

First, go through `_config.yml` and adjust the available settings to your project's standard. When you make changes here, you'll have to kill the `jekyll serve` instance and restart it to see those changes, but that's only the case with the config file.

Next, update some image assets - you'll want to update `favicon.png`, `logo.svg`, and `og_image.png` (used for Like button stories and Shares on Facbeook) in the `static` folder with your own logos.

Next, if you're going to have docs on your site, keep the `_docs` and `docs` folders, if not, you can safely remove them (or you can safely leave them and not include them in your navigation - Jekyll renders all of this before a client views the site anyway, so there's no performance hit from just leaving it there for a future expansion).

Same thing with a blog section, either keep or delete the `_posts` and `blog` folders. 

You can customise your homepage in three parts - the first in the homepage header, which is mostly automatically derived from the elements you insert into your config file. However, you can also specify a series of 'promotional' elements in `_data/promo.yml`. You can read that file for more information.

The second place for your homepage is in `index.md` which contains the bulk of the main content below the header. This is all markdown if you want, but you can use HTML and Jekyll's template tags (called Liquid) in there too. Checkout this folder's index.md for an example of one common template tag that we use on our sites called gridblocks.

The third and last place is in the `_data/powered_by.yml` and `_data/powered_by_highlight.yml` files. Both these files combine to create a section on the homepage that is intended to show a list of companies or apps that are using your project. The `powered_by_highlight` file is a list of curated companies/apps that you want to show as a highlight at the top of this section, including their logos in whatever format you want. The `powered_by` file is a more open list that is just text links to the companies/apps and can be updated via Pull Request by the community. If you don't want these sections on your homepage, just empty out both files and leave them blank.

The last thing you'll want to do is setup your top level navigation bar. You can do this by editing `nav.yml` and keeping the existing title/href/category structure used there. Although the nav is responsive and fairly flexible design-wise, no more than 5 or 6 nav items is recommended. 

## Docs

Editing docs is easy, you can just use lots of markdown. All you need to do is add a new `docname.md` file into the `_docs` folder with the following at the very top of the file (called the Front Matter):

```
---
docid: getting-started
title: Getting started with ProjectName
layout: docs
permalink: /docs/getting-started.html
---
```

Customise these values for each doc (note that the filename of the .md file doesn't actually matter, what is important is the `docid` being unique and the `permalink` correct and unique too).

Then you'll want to add your new doc to the Docs navigation. Just open `_data/nav_docs.yml` and add the `docid` of the doc you just created into the structure in the location that you want it to appear in the nav. Docs can be grouped in the navigation, checkout the skeleton `nav_docs` file that we've provided to see how you can do that. 
